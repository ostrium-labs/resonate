// rbench-temporal: the Temporal driver of rbench (bench/drivers/README.md).
//
// One binary runs a worker, the starter, or both in one process:
//
//	rbench-temporal -mode=all     # worker + starter (default)
//	rbench-temporal -mode=worker  # worker only, until SIGINT/SIGTERM
//	rbench-temporal -mode=start   # starter only, against an external worker
//
// The workload comes from the environment (RBENCH_*, TEMPORAL_ADDRESS,
// TEMPORAL_NAMESPACE, OTEL_*); records go to stdout as JSON Lines, logs to
// stderr. Activity `step` records are written by the process that runs the
// worker.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"
	tlog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
)

type config struct {
	mode      string
	workload  string
	n         int
	k         int
	l         int
	sleepMs   int
	conc      int
	runID     string
	address   string
	namespace string
	taskQueue string

	maxActivities     int
	maxWorkflowTasks  int
	activityPollers   int
	workflowPollers   int
	stickyCacheSize   int
	burstStartWorkers int
	wfTimeout         time.Duration
	activityTimeout   time.Duration
}

func envInt(name string, def int) int {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		fatalf("%s=%q is not an integer", name, v)
	}
	return n
}

func envStr(name, def string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return def
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "rbench-temporal: "+format+"\n", args...)
	os.Exit(2)
}

func logf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "rbench-temporal: "+format+"\n", args...)
}

func main() {
	var c config
	flag.StringVar(&c.mode, "mode", "all", "all (worker + starter), worker, or start")
	flag.StringVar(&c.taskQueue, "task-queue", "rbench", "task queue")
	flag.IntVar(&c.maxActivities, "max-activities", 200, "worker MaxConcurrentActivityExecutionSize")
	flag.IntVar(&c.maxWorkflowTasks, "max-workflow-tasks", 200, "worker MaxConcurrentWorkflowTaskExecutionSize")
	flag.IntVar(&c.activityPollers, "activity-pollers", 8, "worker MaxConcurrentActivityTaskPollers")
	flag.IntVar(&c.workflowPollers, "workflow-pollers", 8, "worker MaxConcurrentWorkflowTaskPollers")
	flag.IntVar(&c.stickyCacheSize, "sticky-cache", 10000, "worker sticky workflow cache size (process-wide)")
	flag.IntVar(&c.burstStartWorkers, "burst-start-workers", 256, "burst: goroutines sending the N start requests")
	flag.DurationVar(&c.activityTimeout, "activity-timeout", 10*time.Second, "activity StartToCloseTimeout (a lost attempt is retried after it)")
	flag.DurationVar(&c.wfTimeout, "wf-timeout", 10*time.Minute, "per-workflow execution timeout")
	flag.Parse()

	c.workload = envStr("RBENCH_WORKLOAD", "fanout")
	c.n = envInt("RBENCH_N", 100)
	c.k = envInt("RBENCH_K", 8)
	c.l = envInt("RBENCH_L", 5)
	c.sleepMs = envInt("RBENCH_SLEEP_MS", 1000)
	c.conc = envInt("RBENCH_CONCURRENCY", 10)
	c.runID = envStr("RBENCH_RUN_ID", fmt.Sprintf("r%d", time.Now().Unix()))
	c.address = envStr("TEMPORAL_ADDRESS", "127.0.0.1:17233")
	c.namespace = envStr("TEMPORAL_NAMESPACE", "default")

	switch c.mode {
	case "all", "worker", "start":
	default:
		fatalf("-mode must be all, worker or start (got %q)", c.mode)
	}
	switch c.workload {
	case "fanout", "chain", "burst":
	default:
		fatalf("RBENCH_WORKLOAD must be fanout, chain or burst (got %q)", c.workload)
	}
	if c.conc < 1 {
		c.conc = 1
	}

	logf("mode=%s workload=%s n=%d k=%d l=%d sleep_ms=%d concurrency=%d run=%s address=%s namespace=%s task_queue=%s",
		c.mode, c.workload, c.n, c.k, c.l, c.sleepMs, c.conc, c.runID, c.address, c.namespace, c.taskQueue)
	logf("starter: activity_timeout=%s wf_timeout=%s burst_start_workers=%d", c.activityTimeout, c.wfTimeout, c.burstStartWorkers)
	logf("worker: max_activities=%d max_workflow_tasks=%d activity_pollers=%d workflow_pollers=%d sticky_cache=%d",
		c.maxActivities, c.maxWorkflowTasks, c.activityPollers, c.workflowPollers, c.stickyCacheSize)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tp, shutdownTracing := setupTracing(ctx)
	tracer := tp.Tracer("rbench-temporal")
	code := run(ctx, c, tracer)
	flushRecords()
	shutdownTracing()
	os.Exit(code)
}

func run(ctx context.Context, c config, tracer trace.Tracer) int {
	tracing, err := opentelemetry.NewTracingInterceptor(opentelemetry.TracerOptions{Tracer: tracer, TextMapPropagator: propagator})
	if err != nil {
		logf("tracing interceptor: %v", err)
		return 2
	}
	sdkLogger := tlog.NewStructuredLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	// The interceptor is a client and a worker interceptor: set on the
	// client, the SDK applies it to the worker built from that client too.
	cl, err := client.Dial(client.Options{
		HostPort:     c.address,
		Namespace:    c.namespace,
		Logger:       sdkLogger,
		Interceptors: []interceptor.ClientInterceptor{tracing},
	})
	if err != nil {
		logf("dial %s: %v", c.address, err)
		return 2
	}
	defer cl.Close()

	var w worker.Worker
	if c.mode != "start" {
		worker.SetStickyWorkflowCacheSize(c.stickyCacheSize)
		w = worker.New(cl, c.taskQueue, worker.Options{
			MaxConcurrentActivityExecutionSize:     c.maxActivities,
			MaxConcurrentWorkflowTaskExecutionSize: c.maxWorkflowTasks,
			MaxConcurrentActivityTaskPollers:       c.activityPollers,
			MaxConcurrentWorkflowTaskPollers:       c.workflowPollers,
		})
		w.RegisterWorkflow(FanOut)
		w.RegisterWorkflow(Chain)
		w.RegisterWorkflow(Single)
		w.RegisterActivity(Noop)
		if err := w.Start(); err != nil {
			logf("worker start: %v", err)
			return 2
		}
		defer w.Stop()
	}

	if c.mode == "worker" {
		logf("worker running on task queue %s; SIGINT/SIGTERM stops it", c.taskQueue)
		<-ctx.Done()
		return 0
	}

	s := &starter{c: c, cl: cl, tracer: tracer}
	t0 := time.Now()
	s.runWorkload(ctx)
	el := time.Since(t0)
	emit(doneRecord{Type: "done", Started: s.started, Completed: s.completed, Failed: s.failed})
	logf("done: started=%d completed=%d failed=%d retries=%d in %s (%.1f wf/s)",
		s.started, s.completed, s.failed, s.retries, el.Round(time.Millisecond), float64(s.completed)/el.Seconds())
	return 0
}

type starter struct {
	c      config
	cl     client.Client
	tracer trace.Tracer

	mu                                  sync.Mutex
	started, completed, failed, retries int
}

func (s *starter) timeoutMs() int { return int(s.c.activityTimeout / time.Millisecond) }

func (s *starter) wfID(i int) string {
	return fmt.Sprintf("%s-%s-%d", s.c.runID, s.c.workload, i)
}

// inflight is one started workflow: its root span and start time.
type inflight struct {
	id      string
	ctx     context.Context
	span    trace.Span
	startNs int64
	run     client.WorkflowRun
	err     error
}

func (s *starter) start(ctx context.Context, i int) *inflight {
	f := &inflight{id: s.wfID(i)}
	f.ctx, f.span = s.tracer.Start(ctx, "rbench.workflow", trace.WithAttributes(
		attribute.String("rbench.run", s.c.runID),
		attribute.String("rbench.workload", s.c.workload),
		attribute.String("rbench.wf_id", f.id),
	))
	opts := client.StartWorkflowOptions{
		ID:                       f.id,
		TaskQueue:                s.c.taskQueue,
		WorkflowExecutionTimeout: s.c.wfTimeout,
	}
	f.startNs = time.Now().UnixNano()
	switch s.c.workload {
	case "fanout":
		f.run, f.err = s.cl.ExecuteWorkflow(f.ctx, opts, FanOut, s.c.k, s.timeoutMs())
	case "chain":
		f.run, f.err = s.cl.ExecuteWorkflow(f.ctx, opts, Chain, s.c.l, s.c.sleepMs, s.timeoutMs())
	case "burst":
		f.run, f.err = s.cl.ExecuteWorkflow(f.ctx, opts, Single, s.timeoutMs())
	}
	s.mu.Lock()
	s.started++
	s.mu.Unlock()
	return f
}

func (s *starter) await(f *inflight) {
	var res WfResult
	err := f.err
	if err == nil {
		err = f.run.Get(f.ctx, &res)
	}
	endNs := time.Now().UnixNano()
	rec := wfRecord{Type: "wf", WfID: f.id, StartNs: f.startNs, EndNs: endNs, Ok: err == nil, Retries: res.Retries}
	if err != nil {
		msg := err.Error()
		rec.Error = &msg
		f.span.RecordError(err)
		f.span.SetStatus(codes.Error, msg)
	}
	f.span.End()
	emit(rec)
	for _, sl := range res.Sleeps {
		// As for every system: sched is when the sleep began, start_ns its due
		// time, so end - start is how late the wake-up was (workflow.Now times).
		sched := sl.StartNs
		due := sl.StartNs + int64(s.c.sleepMs)*1_000_000
		emit(stepRecord{Type: "step", WfID: f.id, Step: sl.Step, Kind: "sleep", SchedNs: &sched, StartNs: due, EndNs: sl.EndNs, Ok: true})
	}
	s.mu.Lock()
	if err != nil {
		s.failed++
	} else {
		s.completed++
	}
	s.retries += res.Retries
	s.mu.Unlock()
}

func (s *starter) runWorkload(ctx context.Context) {
	if s.c.workload == "burst" {
		s.runBurst(ctx)
		return
	}
	// At most C workflows in flight, each start -> Get.
	sem := make(chan struct{}, s.c.conc)
	var wg sync.WaitGroup
	for i := 0; i < s.c.n; i++ {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			logf("interrupted after %d starts", i)
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			s.await(s.start(ctx, i))
		}(i)
	}
	wg.Wait()
}

// runBurst starts all N workflows first (RBENCH_CONCURRENCY is ignored;
// -burst-start-workers goroutines send the start requests), then awaits
// all of them.
func (s *starter) runBurst(ctx context.Context) {
	flights := make([]*inflight, s.c.n)
	idx := make(chan int)
	var wg sync.WaitGroup
	for g := 0; g < s.c.burstStartWorkers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range idx {
				flights[i] = s.start(ctx, i)
			}
		}()
	}
	for i := 0; i < s.c.n; i++ {
		idx <- i
	}
	close(idx)
	wg.Wait()
	logf("burst: %d starts sent", s.c.n)
	for _, f := range flights {
		wg.Add(1)
		go func(f *inflight) {
			defer wg.Done()
			s.await(f)
		}(f)
	}
	wg.Wait()
}
