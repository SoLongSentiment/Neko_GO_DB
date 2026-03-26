package neko_go_db

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type BenchScenario struct {
	Name      string        `json:"name"`
	Ops       int64         `json:"ops"`
	Duration  time.Duration `json:"duration"`
	OpsPerSec float64       `json:"ops_per_sec"`
}

type BenchReport struct {
	Module    string          `json:"module"`
	StartedAt time.Time       `json:"started_at"`
	Duration  time.Duration   `json:"duration"`
	Results   []BenchScenario `json:"results"`
}

func RunBenchmarks(duration time.Duration) (BenchReport, error) {
	if duration <= 0 {
		duration = time.Second
	}

	cfg := DefaultConfig()
	cfg.RedisAddr = "127.0.0.1:" + pickFreePortRuntime()
	cfg.RequestTimeout = 2 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- RunNekoGoDB(ctx, cfg)
	}()

	var client *NekoGoDBClient
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		client, err = NewNekoGoDBClient(cfg)
		if err == nil && client != nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		return BenchReport{}, err
	}
	defer client.Close()
	defer func() {
		cancel()
		select {
		case <-serverErrCh:
		case <-time.After(2 * time.Second):
		}
	}()

	report := BenchReport{
		Module:    "neko_go_db",
		StartedAt: time.Now().UTC(),
		Duration:  duration,
		Results:   make([]BenchScenario, 0, 3),
	}

	startedAt := time.Now()
	var ops int64
	for time.Since(startedAt) < duration {
		key := fmt.Sprintf("bench:set:%d", ops)
		value := fmt.Sprintf("payload-%d", ops)
		if err := client.Set(key, value, time.Minute); err != nil {
			return report, err
		}
		got, err := client.Get(key)
		if err != nil || got != value {
			if err == nil {
				err = fmt.Errorf("value mismatch: got %q want %q", got, value)
			}
			return report, err
		}
		ops++
	}
	report.Results = append(report.Results, BenchScenario{
		Name:      "set_get",
		Ops:       ops,
		Duration:  time.Since(startedAt),
		OpsPerSec: float64(ops) / time.Since(startedAt).Seconds(),
	})

	parallelClient, err := NewNekoGoDBClient(cfg)
	if err != nil {
		return report, err
	}
	defer parallelClient.Close()

	startedAt = time.Now()
	var parallelOps atomic.Int64
	stopCh := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				id := parallelOps.Add(1)
				key := fmt.Sprintf("bench:parallel:%d:%d", worker, id)
				value := fmt.Sprintf("v-%d", id)
				if err := parallelClient.Set(key, value, time.Minute); err != nil {
					return
				}
				got, err := parallelClient.Get(key)
				if err != nil || got != value {
					return
				}
			}
		}(worker)
	}
	time.Sleep(duration)
	close(stopCh)
	wg.Wait()
	parallelCount := parallelOps.Load()
	report.Results = append(report.Results, BenchScenario{
		Name:      "set_get_parallel",
		Ops:       parallelCount,
		Duration:  time.Since(startedAt),
		OpsPerSec: float64(parallelCount) / time.Since(startedAt).Seconds(),
	})

	subClient, err := NewNekoGoDBClient(cfg)
	if err != nil {
		return report, err
	}
	defer subClient.Close()

	sub, err := subClient.Subscribe("bench:pubsub")
	if err != nil {
		return report, err
	}
	defer sub.Close()

	startedAt = time.Now()
	ops = 0
	for time.Since(startedAt) < duration {
		payload := fmt.Sprintf("msg-%d", ops)
		if _, err := client.Publish("bench:pubsub", payload); err != nil {
			return report, err
		}
		_, gotPayload, err := sub.Receive()
		if err != nil || gotPayload != payload {
			if err == nil {
				err = fmt.Errorf("payload mismatch: got %q want %q", gotPayload, payload)
			}
			return report, err
		}
		ops++
	}
	report.Results = append(report.Results, BenchScenario{
		Name:      "pubsub_roundtrip",
		Ops:       ops,
		Duration:  time.Since(startedAt),
		OpsPerSec: float64(ops) / time.Since(startedAt).Seconds(),
	})

	return report, nil
}

func pickFreePortRuntime() string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return strconv.Itoa(16379)
	}
	defer ln.Close()
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}
