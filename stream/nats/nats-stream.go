package nats

import (
	"fmt"
	"sync"
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
	URI       string `json:"uri" doc:"NATS URI (default: nats://nats:4222)"`
	Topic     string `json:"topic" doc:"Topic to consume"`
	NrWorkers int    `json:"nr_workers" doc:"Control number of workers for parallel processing (default: 1)"`
}

func (c *config) Validate() error {
	if c.URI == "" {
		c.URI = "nats://nats:4222"
	}
	if c.Topic == "" {
		return fmt.Errorf("missing topic")
	}
	if c.NrWorkers == 0 {
		c.NrWorkers = 1
	}
	if c.NrWorkers < 1 {
		return fmt.Errorf("nr_workers:%d must be >= 1", c.NrWorkers)
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
	nc       *nats.Conn
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

	//create the message channel and start the workers
	msgChan := make(chan *nats.Msg, s.config.NrWorkers)
	wg := sync.WaitGroup{}
	go func() {
		for {
			log.Debugf("loop")
			select {
			case msg := <-msgChan:
				log.Debugf("Got message(sub:%s,reply:%s,hdr:%+v),data(%s)", msg.Subject, msg.Reply, msg.Header, string(msg.Data))
				wg.Add(1)
				go func(msg *nats.Msg) {
					s.handle(msg)
					wg.Done()
				}(msg)
			case <-time.After(time.Second):
				log.Debugf("no messages yet...")
			}
		} //for main loop
	}()

	defer func() {
		close(msgChan)
		log.Debugf("Closed chan")
		wg.Wait()
		log.Debugf("Workers stopped")
	}()

	//subscribe to write into the message channel
	subscription, err := nc.QueueSubscribeSyncWithChan(
		s.config.Topic,
		s.config.Topic, //Group,
		msgChan)
	if err != nil {
		return fmt.Errorf("failed to subscribe: %v", err)
	}

	log.Debugf("Subscribed to '%s' for processing requests...", s.config.Topic)

	//wait for interrupt then unsubscribe and wait for all workers to terminate
	//todo...
	stop := make(chan bool)
	<-stop
	log.Debugf("stopping")

	if err := subscription.Unsubscribe(); err != nil {
		return fmt.Errorf("failed to unsubscribe: %v", err)
	}
	log.Debugf("Unsubscribed")
	return nil
} //stream.Run()

func (s stream) handle(msg *nats.Msg) {
	log.Debugf("Got task request on: %s", msg.Subject)
	if msg.Reply != "" {
		log.Debugf("Sending reply to \"%s\"", msg.Reply)
		s.nc.Publish(msg.Reply, []byte("Done!"))
	} else {
		log.Debugf("NOT Sending reply")
	}
	time.Sleep(time.Second)
}
