package rabbitmq

import (
	"fmt"
)

const (
	FailedToDial           = "failed to dial RabbitMQ server"
	FailedToOpenChannel    = "failed to open channel"
	FailedToDeclareQueue   = "failed to declare a queue"
	FaileToDeclareExchange = "failed to declare exchange"
	FailedToBindQueue      = "failed to bind queue"
	FailedToPublishMessage = "failed to publish message"
	FailureInConfirmation  = "failed to put channel in confirmation mode"
	ConnectionIsNotOpen    = "connection is not open"
	UnableToStartConsumer  = "unable to start consumer"
)

type RabbitMQError struct {
	Context string
	Detail  string
	Err     error
}

func (r *RabbitMQError) Error() string {
	err := fmt.Sprintf("%s: %v", r.Context, r.Err)
	if r.Detail != "" {
		err += " => " + r.Detail
	}
	return err
}

func RMQError(err error, context, detail string) *RabbitMQError {
	return &RabbitMQError{
		Context: context,
		Err:     err,
		Detail:  detail,
	}
}
