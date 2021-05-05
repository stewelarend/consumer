package nats

import (
	"fmt"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stewelarend/consumer"
	"github.com/stewelarend/logger"
)

var log = logger.New("nats-stream")

func init() {
	consumer.RegisterStream("nats", &config{})
}

type config struct {
	URI   string `json:"uri" doc:"NATS URI (default: nats://nats:4222)"`
	Topic string `json:"topic" doc:"Topic to consume"`
}

func (c *config) Validate() error {
	if c.URI == "" {
		c.URI = "nats://nats:4222"
	}
	if c.Topic == "" {
		return fmt.Errorf("missing topic")
	}
	return nil
}

func (c config) Create(consumer consumer.IConsumer) (consumer.IStream, error) {
	return stream{
		config:   c,
		consumer: consumer,
	}, nil
}

type stream struct {
	config   config
	consumer consumer.IConsumer
}

func (s stream) Run() error {
	var err error
	var nc *nats.Conn
	for i := 0; i < 5; i++ {
		nc, err = nats.Connect(s.config.URI)
		if err == nil {
			break
		}
		fmt.Println("Waiting before connecting to NATS at:", s.config.URI)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return fmt.Errorf("failed to connect to NATS(%s): %v", s.config.URI, err)
	}
	log.Debugf("Connected to NATS(%s)", nc.ConnectedUrl())

	nc.Subscribe(s.config.Topic, func(m *nats.Msg) {
		log.Debugf("Got task request on:", m.Subject)
		if m.Reply != "" {
			log.Debugf("Sending reply to \"%s\"", m.Reply)
			nc.Publish(m.Reply, []byte("Done!"))
		} else {
			log.Debugf("NOT Sending reply")
		}
	})

	log.Debugf("Subscribed to '%s' for processing requests...", s.config.Topic)

	//wait for interrupt then unsubscribe and wait for all workers to terminate
	//todo...
	stop := make(chan bool)
	<-stop

	return nil
} //stream.Run()
