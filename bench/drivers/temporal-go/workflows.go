package main

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/workflow"
)

// SleepRec is one durable timer as the workflow saw it: workflow.Now() just
// before workflow.Sleep and just after it returns. Workflow code may do no
// I/O, so the starter emits the `sleep` step records from these after Get.
type SleepRec struct {
	Step    string `json:"step"`
	StartNs int64  `json:"start_ns"`
	EndNs   int64  `json:"end_ns"`
}

// WfResult is what every rbench workflow returns.
type WfResult struct {
	// Retries is the sum of (attempt - 1) over the workflow's activities,
	// as each activity reports its attempt in its result.
	Retries int        `json:"retries"`
	Sleeps  []SleepRec `json:"sleeps,omitempty"`
}

// NoopIn and NoopOut are the no-op activity's input and output. The
// activity returns its input and the attempt it ran as.
type NoopIn struct {
	Step string `json:"step"`
}

type NoopOut struct {
	Step    string `json:"step"`
	Attempt int32  `json:"attempt"`
}

// Steps have no retries configured beyond the system's default (the
// contract): only the timeout Temporal requires is set. A lost attempt is
// retried after it.
func activityCtx(ctx workflow.Context, timeoutMs int) workflow.Context {
	if timeoutMs <= 0 {
		timeoutMs = 10000
	}
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: time.Duration(timeoutMs) * time.Millisecond,
	})
}

// The workflows take the activity StartToCloseTimeout in ms from the
// starter (flag -activity-timeout), so a separate worker process need not
// agree on it.

// FanOut runs k no-op activities in parallel, then joins.
func FanOut(ctx workflow.Context, k int, timeoutMs int) (WfResult, error) {
	ctx = activityCtx(ctx, timeoutMs)
	futures := make([]workflow.Future, k)
	for i := 0; i < k; i++ {
		futures[i] = workflow.ExecuteActivity(ctx, Noop, NoopIn{Step: fmt.Sprintf("s%d", i)})
	}
	var res WfResult
	for _, f := range futures {
		var out NoopOut
		if err := f.Get(ctx, &out); err != nil {
			return res, err
		}
		res.Retries += int(out.Attempt) - 1
	}
	return res, nil
}

// Chain runs l no-op activities in sequence, each followed by a durable
// timer of sleepMs.
func Chain(ctx workflow.Context, l int, sleepMs int, timeoutMs int) (WfResult, error) {
	ctx = activityCtx(ctx, timeoutMs)
	var res WfResult
	for i := 0; i < l; i++ {
		var out NoopOut
		if err := workflow.ExecuteActivity(ctx, Noop, NoopIn{Step: fmt.Sprintf("s%d", i)}).Get(ctx, &out); err != nil {
			return res, err
		}
		res.Retries += int(out.Attempt) - 1
		t0 := workflow.Now(ctx)
		if err := workflow.Sleep(ctx, time.Duration(sleepMs)*time.Millisecond); err != nil {
			return res, err
		}
		t1 := workflow.Now(ctx)
		res.Sleeps = append(res.Sleeps, SleepRec{Step: fmt.Sprintf("t%d", i), StartNs: t0.UnixNano(), EndNs: t1.UnixNano()})
	}
	return res, nil
}

// Single runs one no-op activity (the burst workload).
func Single(ctx workflow.Context, timeoutMs int) (WfResult, error) {
	ctx = activityCtx(ctx, timeoutMs)
	var out NoopOut
	if err := workflow.ExecuteActivity(ctx, Noop, NoopIn{Step: "s0"}).Get(ctx, &out); err != nil {
		return WfResult{}, err
	}
	return WfResult{Retries: int(out.Attempt) - 1}, nil
}

// Noop returns its input and does no I/O apart from its own `step` record
// (sched_ns and start_ns as the server reports them, end_ns when the body
// returns).
func Noop(ctx context.Context, in NoopIn) (NoopOut, error) {
	info := activity.GetInfo(ctx)
	out := NoopOut{Step: in.Step, Attempt: info.Attempt}
	emit(stepRecord{
		Type:    "step",
		WfID:    info.WorkflowExecution.ID,
		Step:    in.Step,
		Kind:    "activity",
		SchedNs: nsPtr(info.ScheduledTime),
		StartNs: info.StartedTime.UnixNano(),
		EndNs:   time.Now().UnixNano(),
		Ok:      true,
	})
	return out, nil
}

func nsPtr(t time.Time) *int64 {
	if t.IsZero() {
		return nil
	}
	v := t.UnixNano()
	return &v
}
