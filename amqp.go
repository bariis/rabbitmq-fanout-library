package rabbitmq

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

type AMQP struct {
	rabbitmq     *RabbitMQ
	ExchangeName string
	QueueName    string
}

func NewAMQP(rabbitmq *RabbitMQ, exchangeName, queueName string) AMQP {
	return AMQP{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		rabbitmq:     rabbitmq,
	}
}

func (a AMQP) Setup() error {
	channel, err := a.rabbitmq.Channel()
	if err != nil {
		log.Println("failed to open channel")
	}
	defer channel.Close()

	if err := a.declareExchange(channel); err != nil {
		return err
	}

	return nil
}

func (a AMQP) declareExchange(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(
		a.ExchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		log.Printf("failed to declare exchange: %v\n", err)
		return err
	}
	return nil
}

func (a AMQP) StartQueue(channel *amqp.Channel) error {
	channel, err := a.rabbitmq.Channel()
	if err != nil {
		log.Println("failed to open channel")
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
		log.Printf("failed to declare a queue: %v\n", err)
		return err
	}

	if err := channel.QueueBind(
		a.QueueName,
		"",
		a.ExchangeName,
		false,
		nil,
	); err != nil {
		log.Printf("failed to bind queue: %v\n", err)
		return err
	}

	//go a.Consume(channel, queueName)

	return nil
}

func (a AMQP) Push(message string) error {
	channel, err := a.rabbitmq.Channel()
	fmt.Println(channel)
	if err != nil {
		log.Println("failed to open channel")
		return err
	}
	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		log.Println("failed to put channel in confirmation mode")
		return err
	}

	if err := channel.Publish(
		a.ExchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(message),
		},
	); err != nil {
		log.Println("failed to publish message")
		return err
	}

	select {
	case ntf := <-channel.NotifyPublish(make(chan amqp.Confirmation, 1)):
		if !ntf.Ack {
			return errors.New("failed to deliver message to exchange/queue")
		}
	case <-channel.NotifyReturn(make(chan amqp.Return)):
		log.Println("failed to deliver message to exchange/queue")
		return errors.New("failed to deliver message to exchange/queue")
	case <-time.After(10 * time.Second):
		log.Println("message delivery confirmation to exchange/queue timed out")
	}

	return nil
}

func (a AMQP) Consume(channel *amqp.Channel, consumerName string) <-chan amqp.Delivery {
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
		log.Printf("unable to start consumer: %v\n", err)
	}

	return items
	// forever := make(chan bool)

	// go func() {
	// 	for item := range items {
	// 		log.Printf("[%s] received\n", item.Body)
	// 	}
	// }()

	// <-forever
}
