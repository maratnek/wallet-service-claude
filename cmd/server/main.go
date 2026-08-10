package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"wallet-service/internal/serverapp"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := serverapp.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
