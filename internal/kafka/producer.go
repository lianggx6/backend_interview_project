package kafka

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokerAddress, topic string) *Producer {
	logger := log.New(log.Writer(), "kafka-producer ", log.LstdFlags)

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokerAddress),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		MaxAttempts:  10,
		WriteTimeout: 10 * time.Second,
		ReadTimeout:  10 * time.Second,
		BatchTimeout: 100 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
		Async:        false,
		Logger:       logger,
		ErrorLogger:  logger,
		Transport: &kafka.Transport{
			Dial: (&net.Dialer{
				Timeout:   5 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
		},
	}

	return &Producer{
		writer: writer,
	}
}

func (p *Producer) ProduceMessages(ctx context.Context, numMessages int) error {
	log.Printf("Starting to produce %d messages across partitions...", numMessages)

	for i := 0; i < numMessages; i++ {
		key := fmt.Sprintf("key-%d", i)
		value := fmt.Sprintf("message-%d: Hello from Kafka producer at %s", i, time.Now().Format(time.RFC3339))

		message := kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		}

		err := p.writer.WriteMessages(ctx, message)
		if err != nil {
			return fmt.Errorf("failed to write message %d: %w", i, err)
		}

		log.Printf("✓ Produced message %d with key '%s' to partition (hash-distributed)", i, key)
		time.Sleep(100 * time.Millisecond) // Sleep to simulate delay
	}

	log.Printf("Successfully produced all %d messages", numMessages)
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
