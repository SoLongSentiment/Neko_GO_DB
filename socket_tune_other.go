//go:build !windows && !linux

package neko_go_db

import (
	"context"
	"net"
)

func listenWithTuning(ctx context.Context, cfg Config, address string) (net.Listener, error) {
	return (&net.ListenConfig{
		KeepAlive: 3 * cfg.IdleTimeout,
	}).Listen(ctx, "tcp", address)
}
