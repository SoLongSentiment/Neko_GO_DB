package neko_go_db

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNekoGoDBConcurrentSetGet(t *testing.T) {
	harness := newNekoGoDBHarness(t)

	const workers = 8
	const opsPerWorker = 64

	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for op := 0; op < opsPerWorker; op++ {
				key := fmt.Sprintf("stress:%d:%d", worker, op)
				value := fmt.Sprintf("value-%d-%d", worker, op)
				if err := harness.client.Set(key, value, time.Minute); err != nil {
					errCh <- err
					return
				}
				got, err := harness.client.Get(key)
				if err != nil {
					errCh <- err
					return
				}
				if got != value {
					errCh <- fmt.Errorf("value mismatch for %s: got %q want %q", key, got, value)
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent neko_go_db roundtrip failed: %v", err)
		}
	}
}

func BenchmarkNekoGoDB(b *testing.B) {
	harness := newNekoGoDBHarness(b)

	b.Run("set_get", func(b *testing.B) {
		b.ReportAllocs()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("bench:set:%d", i)
			value := fmt.Sprintf("payload-%d", i)
			if err := harness.client.Set(key, value, time.Minute); err != nil {
				b.Fatalf("set: %v", err)
			}
			got, err := harness.client.Get(key)
			if err != nil {
				b.Fatalf("get: %v", err)
			}
			if got != value {
				b.Fatalf("mismatch: got %q want %q", got, value)
			}
		}
		elapsed := time.Since(start)
		if elapsed > 0 {
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "roundtrip/s")
		}
	})

	b.Run("set_get_parallel", func(b *testing.B) {
		b.ReportAllocs()
		b.SetParallelism(4)
		var counter atomic.Int64
		start := time.Now()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				id := counter.Add(1)
				key := fmt.Sprintf("bench:parallel:%d", id)
				value := fmt.Sprintf("v-%d", id)
				if err := harness.client.Set(key, value, time.Minute); err != nil {
					panic(err)
				}
				got, err := harness.client.Get(key)
				if err != nil {
					panic(err)
				}
				if got != value {
					panic(fmt.Sprintf("mismatch: got %q want %q", got, value))
				}
			}
		})
		elapsed := time.Since(start)
		if elapsed > 0 {
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "roundtrip/s")
		}
	})

	b.Run("pubsub_roundtrip", func(b *testing.B) {
		subClient, err := NewNekoGoDBClient(harness.cfg)
		if err != nil {
			b.Fatalf("new sub client: %v", err)
		}
		defer subClient.Close()

		channel := "bench:pubsub"
		sub, err := subClient.Subscribe(channel)
		if err != nil {
			b.Fatalf("subscribe: %v", err)
		}
		defer sub.Close()

		b.ReportAllocs()
		start := time.Now()
		for i := 0; i < b.N; i++ {
			payload := fmt.Sprintf("msg-%d", i)
			b.SetBytes(int64(len(payload)))
			if _, err := harness.client.Publish(channel, payload); err != nil {
				b.Fatalf("publish: %v", err)
			}
			gotChannel, gotPayload, err := sub.Receive()
			if err != nil {
				b.Fatalf("receive: %v", err)
			}
			if gotChannel != channel || gotPayload != payload {
				b.Fatalf("pubsub mismatch: channel=%q payload=%q", gotChannel, gotPayload)
			}
		}
		elapsed := time.Since(start)
		if elapsed > 0 {
			b.ReportMetric(float64(b.N)/elapsed.Seconds(), "msg/s")
		}
	})
}
