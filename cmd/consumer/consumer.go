package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"backend_interview_project/internal/config"
	"backend_interview_project/internal/kafka"

	kafkago "github.com/segmentio/kafka-go"
)

func main() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	cfg := config.Load()
	ctx, cancel := context.WithCancel(context.Background())

	messageHandler := func(ctx context.Context, message kafkago.Message) error {

		// 模拟处理时间
		time.Sleep(50 * time.Millisecond)

		//// 模拟偶尔失败（用于测试重试机制）
		//if message.Offset%7 == 0 {
		//	return fmt.Errorf("simulated processing error")
		//}

		log.Printf("Consume message from partition %d at offset %d: key=%s, value=%s, headers=%s",
			message.Partition,
			message.Offset,
			string(message.Key),
			string(message.Value),
			fmtHeaders(message.Headers))
		return nil
	}

	consumer := kafka.NewConsumer(
		cfg.Kafka.BrokerAddress,
		//"dlq_"+
		cfg.Kafka.Topic,
		cfg.Kafka.GroupID,
		messageHandler,
		kafka.WithMaxConcurrency(cfg.App.MaxWorker),
		kafka.WithMaxRetries(cfg.App.MaxRetries),
	)

	defer func() {
		if err := consumer.Close(); err != nil {
			log.Printf("Error closing consumer: %v", err)
		}
		log.Println("Consumer shutdown complete")
	}()

	go func() {
		if err := consumer.ConsumeMessages(ctx); err != nil && err != context.Canceled {
			log.Printf("Consumer error: %v", err)
		}
	}()

	// Wait for interrupt signal or producer to finish
	select {
	case <-sigChan:
		cancel()
		log.Println("Received interrupt signal, shutting down...")
	case <-ctx.Done():
		log.Println("Context done, shutting down...")
	}
}

func fmtHeaders(headers []kafkago.Header) string {
	var s string
	for _, h := range headers {
		s += fmt.Sprintf("%s=%s; ", h.Key, string(h.Value))
	}
	return s
}
