package neko_go_db

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestNekoGoDBServerRoundTrip(t *testing.T) {
	port := pickFreePort(t)
	cfg := DefaultConfig()
	cfg.RedisAddr = "127.0.0.1:" + port
	cfg.RedisShards = 4
	cfg.RedisConnPool = 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunNekoGoDB(ctx, cfg)
	}()

	var client *NekoGoDBClient
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		client, err = NewNekoGoDBClient(cfg)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("connect client: %v", err)
	}
	defer client.Close()

	if err := client.Set("route:1", "node-a", time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	value, err := client.Get("route:1")
	if err != nil || value != "node-a" {
		t.Fatalf("get mismatch: value=%q err=%v", value, err)
	}

	ok, err := client.SetNX("lock:1", "owner", time.Second)
	if err != nil || !ok {
		t.Fatalf("setnx first failed: ok=%v err=%v", ok, err)
	}
	ok, err = client.SetNX("lock:1", "other", time.Second)
	if err != nil || ok {
		t.Fatalf("setnx second mismatch: ok=%v err=%v", ok, err)
	}

	if _, err := client.HSet("queue:entries", "p1", "payload-1"); err != nil {
		t.Fatalf("hset: %v", err)
	}
	if _, err := client.ZAdd("queue:z", ZMember{Member: "p1", Score: 10}, ZMember{Member: "p2", Score: 20}); err != nil {
		t.Fatalf("zadd: %v", err)
	}
	values, err := client.ZRange("queue:z", 0, -1)
	if err != nil {
		t.Fatalf("zrange: %v", err)
	}
	if len(values) != 2 || values[0] != "p1" || values[1] != "p2" {
		t.Fatalf("unexpected zrange values: %+v", values)
	}

	sub, err := client.Subscribe("events:player")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	if _, err := client.Publish("events:player", "{\"type\":\"match_state\"}"); err != nil {
		t.Fatalf("publish: %v", err)
	}
	channel, payload, err := sub.Receive()
	if err != nil {
		t.Fatalf("receive: %v", err)
	}
	if channel != "events:player" || payload != "{\"type\":\"match_state\"}" {
		t.Fatalf("unexpected pubsub payload: channel=%q payload=%q", channel, payload)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server exit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not stop")
	}
}

func TestNekoGoDBSubscriptionDoesNotExpireOnRequestTimeout(t *testing.T) {
	port := pickFreePort(t)
	cfg := DefaultConfig()
	cfg.RedisAddr = "127.0.0.1:" + port
	cfg.RedisShards = 2
	cfg.RedisConnPool = 2
	cfg.RequestTimeout = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunNekoGoDB(ctx, cfg)
	}()

	var client *NekoGoDBClient
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		client, err = NewNekoGoDBClient(cfg)
		if err == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("connect client: %v", err)
	}
	defer client.Close()

	sub, err := client.Subscribe("events:slow")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	go func() {
		time.Sleep(250 * time.Millisecond)
		_, _ = client.Publish("events:slow", `{"type":"late"}`)
	}()

	channel, payload, err := sub.Receive()
	if err != nil {
		t.Fatalf("receive after timeout window: %v", err)
	}
	if channel != "events:slow" || payload != `{"type":"late"}` {
		t.Fatalf("unexpected pubsub payload: channel=%q payload=%q", channel, payload)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server exit: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("server did not stop")
	}
}

func pickFreePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("pick free port: %v", err)
	}
	defer ln.Close()
	return strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)
}
