package main

import (
	"fmt"
	"time"

	"github.com/stewelarend/consumer"
	"github.com/stewelarend/consumer/controller"
	"github.com/stewelarend/logger"
)

var log = logger.New("main").WithLevel(logger.LevelDebug)

func main() {
	//create a consumer to implement the business logic
	c := consumer.New("myConsumer")
	c.AddStruct("task", TaskReq{})

	//connect to a stream to process events with the consumer
	//e.g. this could be a kafka connector
	stream := stream{next: 0}

	handler := handler{}

	//controller uses the stream and the consumer to process events in controller way
	if err := controller.Run(controller.Config{NrWorkers: 100}, &stream, handler); err != nil {
		panic(err)
	}

	//to terminate, end the stream, then stream must return error on NextEvent()
	//e.g. from <ctrl><C> or other signal handler, call: stream.Close()
	log.Debugf("Ended.")
}

type TaskReq struct {
	Value int
}

func (r TaskReq) Exec(ctx consumer.IContext) error {
	return nil
}

//internal stream to produce events from 0..99
//this simulates a stream like kafka/nats/... even file processing
type stream struct {
	next int
}

func (s *stream) NextEvent() (request interface{}, partitionKey string, err error) {
	if s.next >= 100 {
		return nil, "", fmt.Errorf("end of stream")
	}

	value := s.next
	request = map[string]interface{}{"value": value}
	partitionKey = fmt.Sprintf("%04d", value%2)

	s.next++
	return request, partitionKey, nil
}

type handler struct{}

func (h handler) Handle(eventData interface{}) {
	log.Debugf("event: (%T)%+v", eventData, eventData)
	time.Sleep(time.Millisecond * 400)
}
