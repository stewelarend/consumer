package consumer

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/stewelarend/controller"
	"github.com/stewelarend/util"
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
	Exec(ctx controller.Context, name string, req interface{}) error
}

type HandlerFunc func(controller.Context, interface{}) error

type IHandler interface {
	Exec(ctx controller.Context) error
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

func (consumer consumer) Exec(ctx controller.Context, name string, req interface{}) error {
	handler, ok := consumer.handlers[name]
	if !ok {
		return fmt.Errorf("unknown event(%s)", name)
	}

	//parse req into handler's request struct
	jsonReq, _ := json.Marshal(req)
	newHandler, err := util.StructFromJSON(handler, jsonReq)
	if err != nil {
		return fmt.Errorf("invalid(%s): %v", name, err)
	}
	handler = newHandler.(IHandler)
	if err := handler.Exec(ctx); err != nil {
		return fmt.Errorf("%s failed: %v", name, err)
	}
	return nil
} //consumer.Exec()

//handlerStruct is a wrapper around user functions when the user did not implement a request structure
type handlerStruct struct {
	fnc HandlerFunc
}

func (h handlerStruct) Exec(ctx controller.Context) error {
	fmt.Printf("call h(%+v).fnc=%+v\n", h, h.fnc)
	return h.fnc(ctx, h)
}
