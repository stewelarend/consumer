package nats

import (
	"fmt"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stewelarend/consumer"
	"github.com/stewelarend/logger"
)

var log = logger.New()

func init() {
	consumer.RegisterStream("nats", &config{
		URI:       "nats://nats.4222",
		Topic:     "", //must be configured
		NrWorkers: 1,
	})
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
	s := &stream{
		config:   c,
		consumer: consumer,
	}

	var err error
	for i := 0; i < 5; i++ {
		s.nc, err = nats.Connect(s.config.URI)
		if err == nil {
			break
		}
		fmt.Println("Waiting before connecting to NATS at:", s.config.URI)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS(%s): %v", s.config.URI, err)
	}
	log.Debugf("Connected to NATS(%s)", s.nc.ConnectedUrl())

	//create chan with slots for reading ahead
	//the nr of slots should not be more than nr of workers
	//nats will push into this chan
	s.msgChan = make(chan *nats.Msg, s.config.NrWorkers)

	//subscribe to write into the message channel
	s.subscription, err = s.nc.QueueSubscribeSyncWithChan(
		s.config.Topic,
		s.config.Topic, //Group,
		s.msgChan)
	if err != nil {
		s.nc.Close()
		return nil, fmt.Errorf("failed to subscribe: %v", err)
	}
	log.Debugf("Subscribed to '%s' for processing requests...", s.config.Topic)

	return s, nil
}

type stream struct {
	config       config
	consumer     consumer.IConsumer
	nc           *nats.Conn
	msgChan      chan *nats.Msg
	subscription *nats.Subscription
}

func (s *stream) Close() {
	if s.nc != nil {
		if s.subscription != nil {
			s.subscription.Unsubscribe()
			s.subscription = nil
		}
		s.nc.Close()
		s.nc = nil
		close(s.msgChan)
	}
}

func (s *stream) NextEvent(maxDur time.Duration) (event []byte, partition string, err error) {
	select {
	case msg := <-s.msgChan:
		if msg.Reply == "" {
			log.Debugf("Got message(sub:%s,hdr:%+v),data(%s)", msg.Subject, msg.Reply, msg.Header, string(msg.Data))
			return msg.Data, "", nil
		}

		//consumer from NATS should not be expected to send a reply
		//only RPC sends replies, so it is an error when reply is requested in the event
		//which indicates the process sending this event sent a request instead of an event
		//we respond with an error for such events
		if err := s.nc.Publish(msg.Reply, []byte("reply not supported on this topic")); err != nil {
			log.Errorf("failed to send error reply: %v", err)
		}
		//return no error because the sender was responsible for the error, not us
		//this will skip over and NextEvent will be called again
		log.Debugf("Skipped message(sub:%s,reply:%s,hdr:%+v),data(%s)", msg.Subject, msg.Reply, msg.Header, string(msg.Data))
		return nil, "", nil

	case <-time.After(time.Second):
		log.Debugf("no messages yet...")
		return nil, "", nil
	}
}
