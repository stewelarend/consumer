package consumer

import "fmt"

type IStream interface {
	Run() error
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
