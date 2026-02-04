package config

import (
	"fmt"

	"backend_interview_project/internal/docker"
	"backend_interview_project/internal/kafka"
)

type Config struct {
	Kafka kafka.Config
	App   AppConfig
}

type KafkaConfig struct {
	BrokerAddress string
	Topic         string
	GroupID       string
}

type AppConfig struct {
	NumMessages int // 生产的消息数量
	MaxWorker   int // 最大并发数
	MaxRetries  int // 最大重试次数
}

func Load() *Config {
	return &Config{
		Kafka: kafka.Config{
			BrokerAddress:     fmt.Sprintf("127.0.0.1:%s", docker.KafkaPlaintextPort),
			ControllerAddress: fmt.Sprintf("127.0.0.1:%s", docker.KafkaControllerPort),
			Topic:             "test-topic",
			Partitions:        3,
			GroupID:           "test-consumer-group",
		},
		App: AppConfig{
			NumMessages: 30, // 增加消息数量以更好地展示并发效果
			MaxWorker:   3,  // 每个分区3个worker
			MaxRetries:  3,  // 最多重试3次
		},
	}
}
