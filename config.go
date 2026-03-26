package neko_go_db

import "time"

const DefaultAddr = "127.0.0.1:16379"

type Config struct {
	RedisAddr      string
	RedisShards    int
	RedisConnPool  int
	RequestTimeout time.Duration
	IdleTimeout    time.Duration
}

func DefaultConfig() Config {
	return Config{
		RedisAddr:      DefaultAddr,
		RedisShards:    4,
		RedisConnPool:  16,
		RequestTimeout: 2 * time.Second,
		IdleTimeout:    30 * time.Second,
	}
}
