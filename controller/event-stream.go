package controller

type IEventStream interface {
	NextEvent() (eventData interface{}, partitionKey string, err error)
}
