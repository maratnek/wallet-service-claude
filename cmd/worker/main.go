package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"wallet-service/internal/workerapp"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := workerapp.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
