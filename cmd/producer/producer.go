package main

import (
	"context"
	"log"

	"backend_interview_project/internal/config"
	"backend_interview_project/internal/kafka"
)

func main() {
	cfg := config.Load()
	producer := kafka.NewProducer(cfg.Kafka.BrokerAddress, cfg.Kafka.Topic)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log.Println("Starting producer...")
	if err := producer.ProduceMessages(ctx, cfg.App.NumMessages); err != nil {
		log.Printf("Producer error: %v", err)
	}
	log.Println("Producer finished sending messages")
}
