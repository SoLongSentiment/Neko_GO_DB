package main

import (
	"context"
	"log"

	redis "neko_go_db"
)

func main() {
	cfg := redis.DefaultConfig()
	log.Printf("starting neko_go_db addr=%s shards=%d conn_pool=%d", cfg.RedisAddr, cfg.RedisShards, cfg.RedisConnPool)
	if err := redis.RunNekoGoDB(context.Background(), cfg); err != nil {
		log.Fatal(err)
	}
}
