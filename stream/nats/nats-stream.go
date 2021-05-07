package nats

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	nats "github.com/nats-io/nats.go"
	"github.com/stewelarend/consumer"
	"github.com/stewelarend/consumer/message"
	"github.com/stewelarend/logger"
	"github.com/stewelarend/util"
)

var log = logger.New("nats-stream")

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
	return &stream{
		config:   c,
		consumer: consumer,
	}, nil
}

type stream struct {
	config   config
	consumer consumer.IConsumer
	nc       *nats.Conn
}

func (s *stream) Run() error {
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
		return fmt.Errorf("failed to connect to NATS(%s): %v", s.config.URI, err)
	}
	log.Debugf("Connected to NATS(%s)", s.nc.ConnectedUrl())
	defer func() {
		s.nc.Close()
		s.nc = nil
	}()

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
	subscription, err := s.nc.QueueSubscribeSyncWithChan(
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
	log.Debugf("Got event on: %s", msg.Subject)

	if msg.Reply != "" {
		//consumer from NATS should not be expected to send a reply
		//only RPC sends replies, so it is an error when reply is requested in the event
		//which indicates the process sending this event sent a request instead of an event
		//we respond with an error for such events
		log.Errorf("Sender requested a reply in NATS which is wrong for consumer topics.")
		response := message.Response{
			Message: message.Message{
				Timestamp: time.Now(),
			},
			Result: []message.Result{},
			Data:   nil,
		}
		jsonResponse, _ := json.Marshal(response)
		if err := s.nc.Publish(msg.Reply, jsonResponse); err != nil {
			log.Errorf("failed to send error response to reply: %v", err)
		}
		return
	}

	//todo: on error, push event to error queue...
	var event message.Event
	if err := json.Unmarshal(msg.Data, &event); err != nil {
		log.Errorf("failed to decode JSON event: %v", err)
		return
	}
	if err := event.Validate(); err != nil {
		log.Errorf("invalid event: %v", err)
		return
	}
	log.Debugf("Valid Event: %+v", event)

	//lookup operation
	oper, ok := s.consumer.Oper(event.Type)
	if !ok {
		log.Errorf("unknown event type(%s)", event.Type)
		return
	}

	//parse the event.Request to create a populated oper struct
	newOper, err := util.StructFromValue(oper, event.Data)
	if err != nil {
		log.Errorf("cannot parse %s event data into %T: %v", event.Type, oper, err)
		return
	}
	oper = newOper.(consumer.IHandler)
	if validator, ok := oper.(IValidator); ok {
		if err := validator.Validate(); err != nil {
			log.Errorf("invalid %s event data: %v", event.Type, err)
			return
		}
	}

	//process the parsed request
	if err := oper.Exec(nil); err != nil {
		log.Errorf("oper(%s) failed: %v", event.Type, err)
		return
	}
	log.Debugf("oper(%s) success", event.Type)
}

type IValidator interface {
	Validate() error
}
