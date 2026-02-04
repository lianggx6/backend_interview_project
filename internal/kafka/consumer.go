package kafka

import (
	"context"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

const (
	initialRetryDelay = 100 * time.Millisecond
	maxRetryDelay     = 5 * time.Second
	minCommitInterval = 1 * time.Millisecond // 最小提交间隔
)

const (
	// 默认配置
	defaultMaxConcurrency = 3 // 默认并发数
	defaultMaxRetries     = 3 // 默认最大重试次数
	defaultCommitInterval = time.Second
)

// ConsumeConfig 消费者配置
type ConsumeConfig struct {
	MaxConcurrency int           // 最大并发，所有分区共用
	MaxRetries     int           // 最大重试次数
	CommitInterval time.Duration // 偏移量提交间隔
	// ... 其他配置项
}

// ConsumeConfigOption 消费者配置选项函数
type ConsumeConfigOption func(*ConsumeConfig)

// WithMaxConcurrency 设置每个分区的worker数量
func WithMaxConcurrency(num int) ConsumeConfigOption {
	return func(c *ConsumeConfig) {
		if num > 0 {
			c.MaxConcurrency = num
		}
	}
}

// WithMaxRetries 设置最大重试次数
func WithMaxRetries(retries int) ConsumeConfigOption {
	return func(c *ConsumeConfig) {
		if retries >= 0 {
			c.MaxRetries = retries
		}
	}
}

// WithCommitInterval 设置偏移量提交间隔
func WithCommitInterval(interval time.Duration) ConsumeConfigOption {
	return func(c *ConsumeConfig) {
		if interval > minCommitInterval {
			c.CommitInterval = interval
		}
	}
}

// MessageHandler 消息处理函数类型
type MessageHandler func(ctx context.Context, message kafka.Message) error

// Consumer Kafka消费者
type Consumer struct {
	reader *kafka.Reader
	topic  string
	// 配置
	config ConsumeConfig

	// 消息处理器
	handler MessageHandler

	// 每个分区的worker组
	egMap  map[int]*errgroup.Group
	allEg  errgroup.Group
	cancel context.CancelFunc

	// 偏移量管理
	offsetMu         sync.Mutex
	pendingOffsets   map[int]map[int64]bool // 已接收待处理的offset，在接收不到partition变更信息的情况下，必须记录每个收到的消息的offset用以确认
	maxPendingOffset map[int]int64          // 已接收的最大的offset
	committedOffset  map[int]int64          // 已提交的offset

	// 死信队列
	dlqWriter      *kafka.Writer
	dlqTopic       string
	brokerAddress  string
	dlqInitialized bool
	dlqInitMu      sync.Mutex
}

// NewConsumer 创建新的消费者
// handler: 消息处理函数
// opts: 可选配置项
func NewConsumer(brokerAddress, topic, groupID string, handler MessageHandler, opts ...ConsumeConfigOption) *Consumer {
	// 默认配置
	config := ConsumeConfig{
		MaxConcurrency: defaultMaxConcurrency,
		MaxRetries:     defaultMaxRetries,
		CommitInterval: defaultCommitInterval,
	}

	// 应用配置选项
	for _, opt := range opts {
		opt(&config)
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokerAddress},
		Topic:          topic,
		GroupID:        groupID,
		MinBytes:       1,    // 1B
		MaxBytes:       10e6, // 10MB
		MaxAttempts:    10,
		ReadBackoffMin: 100 * time.Millisecond,
		ReadBackoffMax: 1 * time.Second,
		StartOffset:    kafka.FirstOffset,
		MaxWait:        500 * time.Millisecond,

		// 关键：禁用自动提交，改为手动提交
		CommitInterval: 0,
	})

	dlqTopic := "dlq_" + topic

	consumer := &Consumer{
		reader:           reader,
		topic:            topic,
		config:           config,
		handler:          handler,
		egMap:            make(map[int]*errgroup.Group),
		allEg:            errgroup.Group{},
		committedOffset:  make(map[int]int64),
		pendingOffsets:   make(map[int]map[int64]bool),
		maxPendingOffset: make(map[int]int64),
		dlqTopic:         dlqTopic,
		brokerAddress:    brokerAddress,
		dlqInitialized:   false,
	}

	return consumer
}

// ConsumeMessages 实现并发消费模式
func (c *Consumer) ConsumeMessages(ctx context.Context) error {
	if c.handler == nil {
		return fmt.Errorf("message handler is not set")
	}

	log.Printf("Starting concurrent consumer with %d workers, max %d retries...",
		c.config.MaxConcurrency, c.config.MaxRetries)

	ctx, cancel := context.WithCancel(ctx) // 创建新的取消上下文，在关闭时取消所有任务
	c.cancel = cancel

	c.allEg.Go(func() error {
		c.periodicCommit(ctx) // 定时提交
		return nil
	})

	c.allEg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()

			default:
				// 使用 FetchMessage 而不是 ReadMessage，这样可以手动控制提交
				message, err := c.reader.FetchMessage(ctx)
				if err != nil {
					if ctx.Err() != nil {
						continue
					}
					log.Printf("Error fetching message: %v", err)
					time.Sleep(100 * time.Millisecond) // 错误退避
					continue
				}

				c.markPending(message.Partition, message.Offset)
				eg, exists := c.egMap[message.Partition]
				if !exists {
					eg = &errgroup.Group{}
					eg.SetLimit(c.config.MaxConcurrency)
					c.egMap[message.Partition] = eg
				}

				eg.Go(func() error {
					newCtx := context.Background() // 消费回调函数应当使用新的上下文
					// 处理消息（带重试）
					if err = Retry(newCtx, func() error {
						return c.handler(newCtx, message)
					}, c.config.MaxRetries); err != nil {
						log.Printf("Failed to process message from partition %d offset %d after retries: %v",
							message.Partition, message.Offset, err)
						// 发送消息到死信队列，同样重试
						if sendErr := Retry(newCtx, func() error {
							return c.sendToDLQ(newCtx, message, err)
						}, c.config.MaxRetries); sendErr != nil {
							log.Printf("Failed to send message to DLQ: %v", sendErr)
						}
					}

					// 标记消息处理完成，允许提交该偏移量
					c.markCompleted(message.Partition, message.Offset)
					return nil
				})
			}
		}
	})
	return c.allEg.Wait()
}

// ensureDLQTopic 确保死信队列topic存在
func (c *Consumer) ensureDLQTopic(ctx context.Context) error {
	c.dlqInitMu.Lock()
	defer c.dlqInitMu.Unlock()

	if c.dlqInitialized {
		return nil
	}

	// 连接到Kafka检查topic是否存在
	conn, err := kafka.DialContext(ctx, "tcp", c.brokerAddress)
	if err != nil {
		return fmt.Errorf("failed to dial kafka: %w", err)
	}
	defer conn.Close()

	// 获取controller
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to dial controller: %w", err)
	}
	defer controllerConn.Close()

	// 获取所有topic
	partitions, err := controllerConn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("failed to read partitions: %w", err)
	}

	// 检查DLQ topic是否存在
	topicExists := false
	for _, p := range partitions {
		if p.Topic == c.dlqTopic {
			topicExists = true
			break
		}
	}

	// 如果不存在，创建topic
	if !topicExists {
		log.Printf("DLQ topic %s does not exist, creating...", c.dlqTopic)
		err = controllerConn.CreateTopics(kafka.TopicConfig{
			Topic:             c.dlqTopic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
		if err != nil {
			return fmt.Errorf("failed to create DLQ topic: %w", err)
		}
		log.Printf("Successfully created DLQ topic: %s", c.dlqTopic)
	}

	// 初始化DLQ writer
	c.dlqWriter = &kafka.Writer{
		Addr:     kafka.TCP(c.brokerAddress),
		Topic:    c.dlqTopic,
		Balancer: &kafka.LeastBytes{},
	}

	c.dlqInitialized = true
	return nil
}

// sendToDLQ 将失败的消息发送到死信队列
func (c *Consumer) sendToDLQ(ctx context.Context, message kafka.Message, originalError error) error {
	// 确保DLQ topic存在
	if err := c.ensureDLQTopic(ctx); err != nil {
		return fmt.Errorf("failed to ensure DLQ topic: %w", err)
	}

	// 添加元数据到消息头
	headers := make([]kafka.Header, 0, len(message.Headers)+3)
	headers = append(headers, message.Headers...)
	headers = append(headers,
		kafka.Header{Key: "original_topic", Value: []byte(c.topic)},
		kafka.Header{Key: "original_partition", Value: []byte(fmt.Sprintf("%d", message.Partition))},
		kafka.Header{Key: "original_offset", Value: []byte(fmt.Sprintf("%d", message.Offset))},
		kafka.Header{Key: "error", Value: []byte(originalError.Error())},
		kafka.Header{Key: "failed_at", Value: []byte(time.Now().Format(time.RFC3339))},
	)

	dlqMessage := kafka.Message{
		Key:     message.Key,
		Value:   message.Value,
		Headers: headers,
	}

	err := c.dlqWriter.WriteMessages(ctx, dlqMessage)
	if err != nil {
		return fmt.Errorf("failed to write message to DLQ: %w", err)
	}

	log.Printf("Sent failed message to DLQ topic %s (original: partition=%d, offset=%d)",
		c.dlqTopic, message.Partition, message.Offset)
	return nil
}

// processMessageWithRetry 处理消息并支持重试
func (c *Consumer) processMessageWithRetry(ctx context.Context, message kafka.Message) error {
	err := c.handler(ctx, message)
	if err == nil {
		return nil
	}

	for attempt := 0; attempt < c.config.MaxRetries; attempt++ {
		// 指数退避
		delay := time.Duration(math.Pow(2, float64(attempt))) * initialRetryDelay
		delay = min(delay, maxRetryDelay) // 最大退避5s

		log.Printf("retrying message from partition %d offset %d, retry: %d\n",
			message.Partition, message.Offset, attempt+1)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}

		// 调用外部提供的消息处理函数
		if err = c.handler(ctx, message); err == nil {
			return nil
		}
	}

	return fmt.Errorf("failed after %d retries: %w", c.config.MaxRetries+1, err)
}

// makePending 标记已接收待处理的offset
func (c *Consumer) markPending(partition int, offset int64) {
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()

	// 只在第一次处理该分区时初始化
	if _, exists := c.committedOffset[partition]; !exists {
		// 初始化为第一条消息的前一个 offset
		c.committedOffset[partition] = offset - 1
		log.Printf("Initialized partition %d toBeCommittedOffset to %d (first message offset: %d)",
			partition, offset-1, offset)
	}
	if c.pendingOffsets[partition] == nil {
		c.pendingOffsets[partition] = make(map[int64]bool)
	}
	c.pendingOffsets[partition][offset] = true                                 // 标记待处理
	c.maxPendingOffset[partition] = max(c.maxPendingOffset[partition], offset) // 记录待处理的最大的 offset，提交时不能超过这个offset
}

// markCompleted 标记偏移量处理完成
func (c *Consumer) markCompleted(partition int, offset int64) {
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()

	delete(c.pendingOffsets[partition], offset) // 从待处理中删除
}

// periodicCommit 定期提交偏移量
func (c *Consumer) periodicCommit(ctx context.Context) {
	ticker := time.NewTicker(c.config.CommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := c.commitOffsets(); err != nil {
				log.Printf("Warning: Failed to commit offsets: %v", err)
			}
		}
	}
}

func (c *Consumer) calToBeCommittedOffsets() map[int]int64 {
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()

	nextCommittedOffset := map[int]int64{}
	for partition, offset := range c.committedOffset {
		for !c.pendingOffsets[partition][offset] && offset <= c.maxPendingOffset[partition] {
			offset++
		}
		nextCommittedOffset[partition] = offset - 1
	}
	return nextCommittedOffset
}

// commitOffsets 提交可以安全提交的偏移量
// 策略：只提交连续已处理的偏移量（确保至少一次语义）
func (c *Consumer) commitOffsets() error {
	//log.Println("maxPendingOffset:", c.maxPendingOffset)
	//log.Println("committedOffset:", c.committedOffset)
	//log.Println("pendingOffsets:", c.pendingOffsets)

	readyToCommit := c.calToBeCommittedOffsets() // 先单独计算需要提交的offset，避免在加锁区间内进行commit操作
	successCommit := make(map[int]int64, len(readyToCommit))
	for partition, toBeCommittedOffset := range readyToCommit {
		if toBeCommittedOffset > c.committedOffset[partition] {
			// 提交偏移量
			err := c.reader.CommitMessages(context.Background(), kafka.Message{
				Topic:     c.topic,
				Partition: partition,
				Offset:    toBeCommittedOffset,
			})

			if err != nil {
				log.Printf("Failed to commit offset %d for partition %d: %v", toBeCommittedOffset, partition, err)
				continue
			}

			log.Printf("Committed offset %d for partition %d", toBeCommittedOffset, partition)
			successCommit[partition] = toBeCommittedOffset
		}
	}

	// 更新已提交的偏移量
	c.offsetMu.Lock()
	defer c.offsetMu.Unlock()
	for partition, offset := range successCommit {
		c.committedOffset[partition] = offset
	}

	return nil
}

func (c *Consumer) Close() error {
	log.Println("Start Closing consumer...")

	if err := c.commitOffsets(); err != nil { // 先提交一次偏移量，尽可能保存进度
		log.Printf("Warning: Failed to commit offsets during close: %v", err)
	}

	c.cancel()         // 取消所有任务
	_ = c.allEg.Wait() // 等待自身的协程完成

	// 等待所有分区的协程完成
	for partition, eg := range c.egMap {
		log.Printf("Waiting for partition %d to finish processing...", partition)
		if err := eg.Wait(); err != nil {
			log.Printf("Warning: Failed to wait for partition %d to finish processing: %v", partition, err)
		}
	}

	// 最后一次尝试提交偏移量
	if err := c.commitOffsets(); err != nil {
		log.Printf("Warning: Failed to commit offsets during close: %v", err)
	}

	// 关闭DLQ writer
	if c.dlqWriter != nil {
		if err := c.dlqWriter.Close(); err != nil {
			log.Printf("Warning: Failed to close DLQ writer: %v", err)
		}
	}

	return c.reader.Close()
}
