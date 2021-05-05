package consumer

import (
	"fmt"
	"sort"

	"github.com/stewelarend/config"
)

func New(name string) IConsumer {
	return consumer{
		name:     name,
		handlers: map[string]IHandler{},
	}
}

type IConsumer interface {
	AddStruct(name string, handler IHandler)
	AddFunc(name string, handler HandlerFunc)
	Oper(name string) (IHandler, bool)
	Opers() []string
	Run() error
}

type HandlerFunc func(IContext, interface{}) error

type IHandler interface {
	Exec(ctx IContext) error
}

type consumer struct {
	name     string
	handlers map[string]IHandler
}

func (consumer consumer) AddFunc(name string, handler HandlerFunc) {
	consumer.handlers[name] = handlerStruct{fnc: handler}
}

func (consumer consumer) AddStruct(name string, handler IHandler) {
	consumer.handlers[name] = handler
}

func (consumer consumer) Oper(name string) (IHandler, bool) {
	if f, ok := consumer.handlers[name]; ok {
		return f, true
	}
	return nil, false
}

func (consumer consumer) Opers() []string {
	names := []string{}
	for name := range consumer.handlers {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return names[i] < names[j] })
	return names
}

func (consumer consumer) Run() error {
	//determine configured consumer.stream and run it
	streamConfigs := map[string]interface{}{}
	for n, c := range constructors {
		streamConfigs[n] = c
	}
	streamName, streamConfig, err := config.GetNamedStruct("consumer.stream", streamConfigs)
	if err != nil {
		return fmt.Errorf("cannot get configured rpc.stream: %v", err)
	}
	streamConstructor := streamConfig.(IStreamConstructor)
	stream, err := streamConstructor.Create(consumer)
	if err != nil {
		return fmt.Errorf("failed to create stream(%s): %v", streamName, err)
	}
	return stream.Run()
}

//handlerStruct is a wrapper around user functions when the user did not implement a request structure
type handlerStruct struct {
	fnc HandlerFunc
}

func (h handlerStruct) Exec(ctx IContext) error {
	fmt.Printf("call h(%+v).fnc=%+v\n", h, h.fnc)
	return h.fnc(ctx, h)
}
