//go:build linux

package neko_go_db

import (
	"context"
	"net"
	"syscall"
)

func listenWithTuning(ctx context.Context, cfg Config, address string) (net.Listener, error) {
	return (&net.ListenConfig{
		KeepAlive: 3 * cfg.IdleTimeout,
		Control: func(_, _ string, c syscall.RawConn) error {
			var controlErr error
			err := c.Control(func(fd uintptr) {
				_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
			})
			if err != nil {
				controlErr = err
			}
			return controlErr
		},
	}).Listen(ctx, "tcp", address)
}
