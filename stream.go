package consumer

import (
	"fmt"
	"time"
)

type IStream interface {
	//NextEvent return success with an event as: data=[]byte, partitionKey="" or "..." and err=nil
	//if maxDur > 0 and no event by that time, return nil,"",err=nil
	//if closed or cannot get event, return nil,"",err!=nil
	NextEvent(maxDir time.Duration) (eventData []byte, partitionKey string, err error)

	//after close was called, NextEvent may only return buffered events and then should
	//return err!=nil
	Close()
}

type IStreamConstructor interface {
	Validate() error
	Create(IConsumer) (IStream, error)
}

var constructors = map[string]IStreamConstructor{}

func RegisterStream(name string, constructor IStreamConstructor) {
	constructors[name] = constructor
	fmt.Printf("Registered constructor[%s] = %+v\n", name, constructor)
}

func NewStream(service IConsumer, name string, config map[string]interface{}) (IStream, error) {
	constructor, ok := constructors[name]
	if !ok {
		panic(fmt.Errorf("stream(%s) constructor is not registered", name))
	}
	return constructor.Create(service)
}
