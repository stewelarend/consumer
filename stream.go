package consumer

import (
	"fmt"
	"time"

	"github.com/stewelarend/util"
)

//IStream interface can be implemented to stream from files, kafka, nats, ...
//see github.com/stewelarend/consumer/stream/xxx/... for examples
//you can create your own streams and add them with RegisterStream()
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
	Create() (IStream, error)
}

var constructors = map[string]IStreamConstructor{}

func RegisterStream(name string, constructor IStreamConstructor) {
	constructors[name] = constructor
	fmt.Printf("Registered constructor[%s] = %+v\n", name, constructor)
}

func NewStream(name string, config map[string]interface{}) (IStream, error) {
	constructor, ok := constructors[name]
	if !ok {
		return nil, fmt.Errorf("stream(%s) is not registered (forgotten import?)", name)
	}
	configured, err := util.StructFromValue(constructor, config)
	if err != nil {
		return nil, fmt.Errorf("stream(%s): %v", name, err)
	}
	return configured.(IStreamConstructor).Create()
}
