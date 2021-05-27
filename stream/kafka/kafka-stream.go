package kafka

import (
	"fmt"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stewelarend/consumer"
	"github.com/stewelarend/logger"
)

var log = logger.New()

func init() {
	consumer.RegisterStream("kafka", &Config{
		NrWorkers: 1,
		Offset:    "latest",
	})
}

type Config struct {
	Servers   []string `json:"servers" doc:"Kafka server adresses to connect to, e.g. \"1.2.3.4:11111\""`
	NrWorkers int      `json:"workers" doc:"Number of concurrent worker processes (default: 1)"`
	Topics    []string `json:"topics" doc:"Topics to subscriber to (require at least one)"`
	GroupID   string   `json:"group_id" doc:"Subscription group (required)"`
	Offset    string   `json:"offset" doc:"Kafka offset (e.g. latest)"`
	//partition fields
}

func (c *Config) Validate() error {
	if len(c.Servers) == 0 {
		return fmt.Errorf("missing servers")
	}
	if c.NrWorkers == 0 {
		c.NrWorkers = 1
	}
	if c.NrWorkers < 1 {
		return fmt.Errorf("negative nr_workers:%d", c.NrWorkers)
	}
	if len(c.Topics) == 0 {
		return fmt.Errorf("missing topics")
	}
	for _, t := range c.Topics {
		if len(t) == 0 {
			return fmt.Errorf("blank topic in %+v", c.Topics)
		}
	}
	if c.GroupID == "" {
		return fmt.Errorf("missing group_id")
	}
	if c.Offset == "" {
		return fmt.Errorf("missing offset")
	}
	return nil
}

func (c Config) Create(consumer consumer.IConsumer) (consumer.IStream, error) {
	s := kafkaStream{
		config:   c,
		consumer: consumer,
	}
	kafkaConfig := kafka.ConfigMap{
		"bootstrap.servers": strings.Join(c.Servers, ","),
		"group.id":          c.GroupID,
		"auto.offset.reset": c.Offset,
	}
	var err error
	if s.kafka, err = kafka.NewConsumer(&kafkaConfig); err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %v", err)
	}
	if err := s.kafka.SubscribeTopics(s.config.Topics, nil); err != nil {
		s.kafka.Close()
		return nil, fmt.Errorf("failed to subscriber to topics(%+v): %v", c.Topics, err)
	}
	return s, nil
}

type kafkaStream struct {
	config   Config
	consumer consumer.IConsumer
	kafka    *kafka.Consumer
}

func (s kafkaStream) Close() {
	if s.kafka != nil {
		s.kafka.Close()
		s.kafka = nil
	}
}

func (s kafkaStream) NextEvent(maxDur time.Duration) (eventData []byte, partitionKey string, err error) {
	if s.kafka == nil {
		return nil, "", fmt.Errorf("kafka stream closed")
	}

	//poll for new kafka events/messages (-1 to block without timeout)
	if maxDur <= 0 {
		maxDur = time.Duration(-1) //required by kafka for indefinite blocking...
	}
	kafkaMessage, err := s.kafka.ReadMessage(maxDur)
	if err != nil {
		//no message yet
		return nil, "", nil
	}

	return kafkaMessage.Value, kafkaMessage.TopicPartition.String(), nil
} //stream.NextEvent()
