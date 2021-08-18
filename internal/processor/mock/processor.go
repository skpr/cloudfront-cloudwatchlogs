package mock

import "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"

// Processor provides a mock processor.
type Processor struct {
	events []types.InputLogEvent
}

// NewProcessor creates a new mock processor.
func NewProcessor() *Processor {
	return &Processor{}
}

// Process the event.
func (p *Processor) Process(event types.InputLogEvent) error {
	p.events = append(p.events, event)
	return nil
}

// GetEvents gets the events.
func (p *Processor) GetEvents() []types.InputLogEvent {
	return p.events
}
