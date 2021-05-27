package controller

import "github.com/stewelarend/consumer"

type IEventHandler interface {
	Handle(ctx consumer.IContext, eventData []byte) error
}
