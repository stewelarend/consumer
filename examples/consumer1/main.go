package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/stewelarend/consumer"
	"github.com/stewelarend/controller"
	"github.com/stewelarend/logger"
)

var log = logger.New().WithLevel(logger.LevelDebug)

var eventChan chan int

func main() {
	//connect to a stream that provides events for the consumer to process
	//e.g. this could be a Kafka, RabbitMQ or NATS connector or it could read events from a JSON or CSV file etc...
	//for this example, our stream is a simple struct that generates N events from 0..N-1
	stream := stream{n: 100}

	//create a consumer to implement the business logic in request handlers
	//here TaskReq{} is the expected request event data
	//and it implements consumer.IHandler to processes the event data (without a response)
	//one can have many request structs each doing something differently
	c := consumer.New("myConsumer")
	c.AddStruct("task", TaskReq{})

	//the handler sits between the stream and the consumer
	//it decodes the messages from the stream, determine the type of request and asks c to execute that event
	//here you can decide if your message format is ASN.1 or JSON or Protobuf etc and your message
	//header structure that indicates the partitionKey and type...
	//partitionKey however can also come from the stream
	//if the stream does not deal with partitionKey (as Kafka does) then records for the same
	//partition may land at different consumers, and you should only have one consumer per topic,
	//e.g. you could have your producers send to a topic using hash of the partition key in the
	//topic, e.g. topic=Sprintf("user_actions_%02d", hash(user.id) % 5)
	//but then it is your responsibility to ensure there are consumers subscribing to
	//   user_action_00, user_action_01, ..., user_action_04
	//In kafka for one, you can send to "user_action" and specify the partition key=user.id
	//then the messages with the same partition key goes to the same consumer and kafka
	//split it among the connected consumers, no matter how many there are...
	handler := handler{consumer: c}

	//our handler push to a chan so we can verify the nr of events consumed
	//consume that chan here for verification, which will stop when eventChan is closed
	eventTotal := 0
	eventChan = make(chan int)
	eventWg := sync.WaitGroup{}
	eventWg.Add(1)
	go func() {
		for i := range eventChan {
			eventTotal += i
		}
		eventWg.Done()
	}()

	//controller uses the stream and the handler to process events in controller way
	//doing concurrent processing on events from different partition keys
	//a partition key is simply a string to serialise related events
	//e.g. if you process user events, the user id will feature in the partition key
	//to ensure all events on the same user goes to the same worker queue
	//when using a Kafka stream, kafka also implements partitioning
	//and if there are 5 consumers connected to kafka, it will hash the keys and give
	// 1/5 to each connected consumer. That means each consumer will still get many distinct
	//partition key values, but the same values will go to the same consumer
	//Your stream should return that same partitionKey to the controller to further serialise
	//internally, but still allow different keys to run in parallel
	//On the other hand, if the partition key values are only "0", "1", ..., "10" for example
	//and you have 5 consumers connected to the stream, and assuming the hash function is
	//perfect, your controller will only get 2 or 3 different partition values at most,
	//so then if you run with 100 workers, they will all remain idle except for 2 or 3 of
	//them that will process all the events. The consumer will continue grabbing up to 100
	//events from kafka (100=NrWorkers) because it has capacity for 100, but since your
	//partitionKey value is limited, very little parallel processing will happen.
	//To fix this, use a much wider partitionkey, e.g. unique ids on data records,
	//to ensure all the workers can run in parallel. If your data really has a limited key
	//range like (1..10) then it is pointless to have 5 consumers with 100 workers each :-)
	//because you can do at most 10 in parallel. So total nr of consumer instances multiplied
	//by nr of workers in each instance, should not be more than the range of your partition
	//key values, in fact it could be much less, but it should reflect the number of parallel
	//tasks you want to do.
	if err := controller.Run(controller.Config{NrWorkers: 100}, &stream, handler); err != nil {
		panic(err)
	}

	//to terminate, end the stream, then stream must return error on NextEvent()
	//e.g. from <ctrl><C> or other signal handler, call: stream.Close()
	//in this example, it happens when counted stream.next>=stream.n
	log.Debugf("Ended.")

	//check that all events were processed
	close(eventChan)
	eventWg.Wait()
	log.Debugf("total=%d", eventTotal)
	if eventTotal != 4950 {
		log.Errorf("count=%d != 4950", eventTotal)
	}
}

//TaskReq is the expected eventData structure
//and it implements the consumer.IHandler
type TaskReq struct {
	Value int
}

func (r TaskReq) Exec(ctx controller.Context) error {
	log.Debugf("task %+v", r)
	eventChan <- r.Value
	return nil
}

//internal stream to produce events from 0..99
//this simulates a stream like kafka/nats/... even file processing
type stream struct {
	n    int
	next int
}

func (s *stream) NextEvent(maxDur time.Duration) (eventData []byte, partitionKey string, err error) {
	if s.next >= s.n {
		return nil, "", fmt.Errorf("end of stream after %d events", s.n)
	}

	//simulate a eventData as a network message from some producer...
	value := s.next
	eventData, _ = json.Marshal(map[string]interface{}{
		"type": "task",
		"request": map[string]interface{}{
			"value": value,
		},
	})

	partitionKey = fmt.Sprintf("%04d", value%2)

	s.next++

	return eventData, partitionKey, nil
}

type handler struct {
	consumer consumer.IConsumer
}

type Message struct {
	Type    string      `json:"type"`
	Request interface{} `json:"request"`
}

//Handle is called in a background worker
//it may panic on error or return error value...
func (h handler) Handle(ctx controller.Context, eventData []byte) error {
	//parse event data from stream, a JSON message, into our own message struct
	msg := Message{}
	if err := json.Unmarshal(eventData, &msg); err != nil {
		panic(fmt.Errorf("ERROR: cannot parse message as JSON: %v", err))
	}

	//todo: start background + context...
	// auditEvent := audit.Event{
	// 	Header: audit.EventHeader{
	// 		StartTime: time.Now(),
	// 		Type:      msg.Type,
	// 		Trace:     string(msg.TraceID),
	// 	},
	// 	Data: nil,
	// }
	//errCode := "ERROR"
	err := h.consumer.Exec(ctx, msg.Type, msg.Request)
	if err != nil {
		return fmt.Errorf("type(%s) failed: %v", msg.Type, err)
	}

	// if err != nil {
	// 	auditEvent.Result = audit.Result{
	// 		Success: false,
	// 		ErrCode: errCode,
	// 		ErrDesc: fmt.Sprintf("%v", err),
	// 	}
	// } else {
	// 	auditEvent.Result = audit.Result{
	// 		Success: true,
	// 	}
	// }

	// //todo: set request data when parsing generically here before handler!
	// //todo: add metrics to audits
	// audit.Write(auditEvent)
	return nil
}
