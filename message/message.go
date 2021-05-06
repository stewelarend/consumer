package message

import (
	"fmt"
	"time"
)

type Message struct {
	Timestamp time.Time `json:"timestamp" doc:"Time when message is sent"`
	TraceID   string    `json:"trace_id,omitempty" doc:"Trace ID is optional. If not specified, the service may assign its own value and propogate. If specified, must be echoed in reply or further messages."`
}

func (m Message) Validate() error {
	return nil
}

type Event struct {
	Message
	Type string      `json:"type" doc:"Type of event."`
	Data interface{} `json:"data,omitempty" doc:"Optional event data."`
}

func (evt Event) Validate() error {
	if err := evt.Message.Validate(); err != nil {
		return fmt.Errorf("invalid message: %v", err)
	}
	if evt.Type == "" {
		return fmt.Errorf("missing type")
	}
	return nil
}

type Request struct {
	Message
	TTL       time.Duration `json:"ttl,omitempty" doc:"Time that sender is prepared to wait after timestamp for the response (> 0)"`
	Operation string        `json:"operation" doc:"Operation name is required."`
	Data      interface{}   `json:"data,omitempty" doc:"Optional request data."`
}

func (req Request) Validate() error {
	if err := req.Message.Validate(); err != nil {
		return fmt.Errorf("invalid message: %v", err)
	}
	if req.TTL < 0 {
		return fmt.Errorf("negative ttl=%v", req.TTL)
	}
	if req.Timestamp.Add(req.TTL).Before(time.Now()) {
		return fmt.Errorf("expired at %v+%v=%v", req.Timestamp, req.TTL, req.Timestamp.Add(req.TTL))
	}
	if req.Operation == "" {
		return fmt.Errorf("missing operation")
	}
	return nil
}

type Response struct {
	Message
	Result []Result    `json:"result" doc:"Result is present in ALL responses with at least one element. When failing because of another failure, include the other failures after your own result. Result[0] is always the result for this response."`
	Data   interface{} `json:"data,omitempty" doc:"Optional data."`
}

func (res Response) Validate() error {
	if err := res.Message.Validate(); err != nil {
		return fmt.Errorf("invalid message: %v", err)
	}
	if len(res.Result) < 1 {
		return fmt.Errorf("missing result")
	}
	for i, result := range res.Result {
		if err := result.Validate(); err != nil {
			return fmt.Errorf("invalid result[%d]:%+v: %v", i, result, err)
		}
	}
	return nil
}

type Result struct {
	Timestamp time.Time     `json:"timestamp" doc:"Copied from the Request.Timestamp"`
	StartTime time.Time     `json:"start_time" doc:"Time when processing started (>=Timestamp)"`
	Duration  time.Duration `json:"duration" doc:"Duration spent to get to the result (now - StartTime)"`
	Service   string        `json:"service" doc:"Name of the service that sent this result"`
	Operation string        `json:"operation" doc:"Name of the service operation that sent this result (echo from Request.Operation)"`
	Code      int           `json:"code" doc:"Numeric result code defined in the scope of the service and operation (0 always success)"`
	Name      string        `json:"name" doc:"Name of the result code (0 always \"success\" in lowercase)"`
	Details   string        `json:"details,omitempty" doc:"Optional more details about the result"`
}

func (result Result) Validate() error {
	if result.Duration < 0 {
		return fmt.Errorf("negativ duration:%v", result.Duration)
	}
	if result.StartTime.Before(result.Timestamp) {
		return fmt.Errorf("start=%v before timestamp=%v", result.StartTime, result.Timestamp)
	}
	if result.StartTime.Add(result.Duration).After(time.Now()) {
		return fmt.Errorf("future start_time %v+%v=%v", result.StartTime, result.Duration, result.StartTime.Add(result.Duration))
	}
	if result.Service == "" {
		return fmt.Errorf("missing service")
	}
	if result.Operation == "" {
		return fmt.Errorf("missing operation")
	}
	if result.Name == "" {
		return fmt.Errorf("missing name")
	}
	return nil
}
