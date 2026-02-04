package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"backend_interview_project/internal/config"
	"backend_interview_project/internal/docker"
	"backend_interview_project/internal/kafka"
)

func main() {
	log.Println("Starting Kafka producer/consumer application...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Start Kafka container
	log.Println("Starting Kafka container programmatically...")
	kafkaManager, err := docker.NewKafkaManager()
	if err != nil {
		log.Fatalf("Failed to create Kafka manager: %v", err)
	}
	if err := kafkaManager.Start(ctx); err != nil {
		log.Fatalf("Failed to start Kafka: %v", err)
	}

	cfg := config.Load()

	if err := kafka.RecreateTopic(ctx, cfg.Kafka); err != nil {
		log.Printf("Warning: Failed to create topic with %d partitions: %v", cfg.Kafka.Partitions, err)
		panic(err)
	}

	defer func() {
		kafkaManager.Close()
		log.Println("Application shutdown complete")
	}()

	select {
	case <-sigChan:
		log.Println("Received interrupt signal, shutting down...")
		cancel()
	case <-ctx.Done():
		log.Println("Context done, shutting down...")
	}
}
