package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// The JSON Lines records of bench/drivers/README.md, on stdout. Logs go to
// stderr.

type wfRecord struct {
	Type    string  `json:"type"`
	WfID    string  `json:"wf_id"`
	StartNs int64   `json:"start_ns"`
	EndNs   int64   `json:"end_ns"`
	Ok      bool    `json:"ok"`
	Error   *string `json:"error"`
	Retries int     `json:"retries"`
}

type stepRecord struct {
	Type    string `json:"type"`
	WfID    string `json:"wf_id"`
	Step    string `json:"step"`
	Kind    string `json:"kind"`
	SchedNs *int64 `json:"sched_ns"`
	StartNs int64  `json:"start_ns"`
	EndNs   int64  `json:"end_ns"`
	Ok      bool   `json:"ok"`
}

type doneRecord struct {
	Type      string `json:"type"`
	Started   int    `json:"started"`
	Completed int    `json:"completed"`
	Failed    int    `json:"failed"`
}

var (
	outMu sync.Mutex
	out   = bufio.NewWriterSize(os.Stdout, 1<<16)
)

// emit writes one record as one line. Lines are buffered and flushed by
// flushRecords (and whenever the buffer fills).
func emit(v any) {
	b, err := json.Marshal(v)
	if err != nil {
		fmt.Fprintf(os.Stderr, "rbench-temporal: encode record: %v\n", err)
		return
	}
	outMu.Lock()
	out.Write(b)
	out.WriteByte('\n')
	outMu.Unlock()
}

func flushRecords() {
	outMu.Lock()
	out.Flush()
	outMu.Unlock()
}
