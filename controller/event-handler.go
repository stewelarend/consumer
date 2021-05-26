package controller

type IEventHandler interface {
	Handle(eventData interface{})
}
