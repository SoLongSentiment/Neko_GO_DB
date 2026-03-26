package neko_go_db

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"
)

type nekoRedisHarness struct {
	cfg    Config
	client *NekoGoDBClient
	cancel context.CancelFunc
	errCh  chan error
}

func newNekoGoDBHarness(tb testing.TB) *nekoRedisHarness {
	tb.Helper()

	cfg := DefaultConfig()
	cfg.RedisAddr = "127.0.0.1:" + pickFreePortTB(tb)
	cfg.RedisShards = 4
	cfg.RedisConnPool = 32
	cfg.RequestTimeout = 2 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunNekoGoDB(ctx, cfg)
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
		cancel()
		tb.Fatalf("connect neko_go_db client: %v", err)
	}
	if client == nil {
		cancel()
		tb.Fatalf("neko_go_db client is nil")
	}

	harness := &nekoRedisHarness{
		cfg:    cfg,
		client: client,
		cancel: cancel,
		errCh:  errCh,
	}

	tb.Cleanup(func() {
		_ = harness.client.Close()
		harness.cancel()
		select {
		case err := <-harness.errCh:
			if err != nil && !errors.Is(err, context.Canceled) {
				tb.Fatalf("neko_go_db shutdown: %v", err)
			}
		case <-time.After(2 * time.Second):
			tb.Fatalf("neko_go_db did not stop")
		}
	})

	return harness
}

func pickFreePortTB(tb testing.TB) string {
	tb.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("pick free port: %v", err)
	}
	defer ln.Close()
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}
