package rabbitmq

import (
	"errors"
	"log"
	"time"

	"github.com/streadway/amqp"
)

const (
	ExchangeType = "fanout"
)

type AMQP struct {
	rabbitmq     *RabbitMQ
	ContentType  string
	ExchangeName string
	QueueName    string
}

func NewAMQP(rabbitmq *RabbitMQ, exchangeName, queueName, contentType string) AMQP {
	return AMQP{
		ExchangeName: exchangeName,
		ContentType:  contentType,
		QueueName:    queueName,
		rabbitmq:     rabbitmq,
	}
}

func (a AMQP) Setup() error {
	channel, err := a.rabbitmq.Channel()
	if err != nil {
		return RMQError(err, FailedToOpenChannel, "")
	}
	defer channel.Close()

	if err := a.declareExchange(channel); err != nil {
		return RMQError(err, FaileToDeclareExchange, "")
	}

	return nil
}

func (a AMQP) declareExchange(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(
		a.ExchangeName,
		ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}
	return nil
}

func (a AMQP) StartQueue(channel *amqp.Channel) error {
	channel, err := a.rabbitmq.Channel()
	if err != nil {
		return RMQError(err, FailedToOpenChannel, "")
	}
	defer channel.Close()

	if _, err := channel.QueueDeclare(
		a.QueueName,
		false,
		false,
		true,
		false,
		nil,
	); err != nil {
		return RMQError(err, FailedToDeclareQueue, "")
	}

	if err := channel.QueueBind(
		a.QueueName,
		"",
		a.ExchangeName,
		false,
		nil,
	); err != nil {
		return RMQError(err, FailedToBindQueue, "")
	}
	return nil
}

func (a AMQP) Push(message string) error {
	channel, err := a.rabbitmq.Channel()
	if err != nil {
		return RMQError(err, FailedToOpenChannel, "")
	}
	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return RMQError(err, FailureInConfirmation, "")
	}

	if err := channel.Publish(
		a.ExchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: a.ContentType,
			Body:        []byte(message),
		},
	); err != nil {
		return RMQError(err, FailedToPublishMessage, "")
	}

	select {
	case ntf := <-channel.NotifyPublish(make(chan amqp.Confirmation, 1)):
		if !ntf.Ack {
			return errors.New("failed to deliver message to exchange/queue")
		}
	case <-channel.NotifyReturn(make(chan amqp.Return)):
		return errors.New("failed to deliver message to exchange/queue")
	case <-time.After(10 * time.Second):
		log.Println("message delivery confirmation to exchange/queue timed out")
	}

	return nil
}

func (a AMQP) Consume(channel *amqp.Channel, consumerName string) (<-chan amqp.Delivery, error) {
	items, err := channel.Consume(
		a.QueueName,
		consumerName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, RMQError(err, UnableToStartConsumer, consumerName)
	}

	return items, nil
}
