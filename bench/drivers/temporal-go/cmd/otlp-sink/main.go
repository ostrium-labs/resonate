// otlp-sink: a minimal OTLP/gRPC trace receiver for checking that spans
// arrive (a debugging aid, not a collector). It counts spans per
// service.name and span name, and prints the summary to stderr on SIGINT or
// SIGTERM.
//
//	go run ./cmd/otlp-sink -listen 127.0.0.1:4317
package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"

	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
)

type sink struct {
	coltrace.UnimplementedTraceServiceServer
	mu       sync.Mutex
	counts   map[string]int
	traces   map[string]map[string]bool // trace id -> services seen
	examples []string
}

func (s *sink) Export(_ context.Context, req *coltrace.ExportTraceServiceRequest) (*coltrace.ExportTraceServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, rs := range req.ResourceSpans {
		svc := "?"
		for _, a := range rs.GetResource().GetAttributes() {
			if a.Key == "service.name" {
				svc = a.Value.GetStringValue()
			}
		}
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				s.counts[svc+" | "+sp.Name]++
				tid := fmt.Sprintf("%x", sp.TraceId)
				if s.traces[tid] == nil {
					s.traces[tid] = map[string]bool{}
				}
				s.traces[tid][svc] = true
				if sp.Name == "rbench.workflow" && len(s.examples) < 3 {
					attrs := ""
					for _, a := range sp.Attributes {
						attrs += fmt.Sprintf(" %s=%s", a.Key, a.Value.GetStringValue())
					}
					s.examples = append(s.examples, svc+" rbench.workflow trace="+tid+attrs)
				}
			}
		}
	}
	return &coltrace.ExportTraceServiceResponse{}, nil
}

func main() {
	listen := flag.String("listen", "127.0.0.1:4317", "address to listen on")
	flag.Parse()
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		fmt.Fprintln(os.Stderr, "otlp-sink:", err)
		os.Exit(2)
	}
	s := &sink{counts: map[string]int{}, traces: map[string]map[string]bool{}}
	g := grpc.NewServer()
	coltrace.RegisterTraceServiceServer(g, s)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() { <-ch; g.Stop() }()
	fmt.Fprintln(os.Stderr, "otlp-sink: listening on", *listen)
	_ = g.Serve(ln)
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.counts))
	for k := range s.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(os.Stderr, "%6d  %s\n", s.counts[k], k)
	}
	multi := 0
	for _, svcs := range s.traces {
		if len(svcs) > 1 {
			multi++
		}
	}
	fmt.Fprintf(os.Stderr, "traces: %d, spanning more than one service: %d\n", len(s.traces), multi)
	for _, e := range s.examples {
		fmt.Fprintln(os.Stderr, e)
	}
}
