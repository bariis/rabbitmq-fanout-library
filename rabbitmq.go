package rabbitmq

import (
	"log"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type RabbitMQ struct {
	mux        sync.RWMutex
	connection *amqp.Connection
	AMQPUri    string
}

func New(amqpURI string) *RabbitMQ {
	return &RabbitMQ{AMQPUri: amqpURI}
}

func (r *RabbitMQ) Connect() error {
	conn, err := amqp.Dial(r.AMQPUri)
	if err != nil {
		return RMQError(err, FailedToDial, "")
	}
	r.connection = conn

	go r.reconnect()

	return nil
}

func (r *RabbitMQ) Channel() (*amqp.Channel, error) {
	if r.connection == nil {
		if err := r.Connect(); err != nil {
			return nil, RMQError(err, ConnectionIsNotOpen, "")
		}
	}

	channel, err := r.connection.Channel()
	if err != nil {
		return nil, RMQError(err, FailedToOpenChannel, "")
	}
	return channel, nil
}

func (r *RabbitMQ) reconnect() {
WATCH:
	conErr := <-r.connection.NotifyClose(make(chan *amqp.Error))
	if conErr != nil {
		log.Println("CRITICAL: Connection dropped, reconnecting")

		var err error

		for i := 1; i <= 7200; i++ {
			r.mux.RLock()
			r.connection, err = amqp.Dial(r.AMQPUri)
			if err == nil {
				log.Println("INFO: Reconnected")
				goto WATCH
			}
			time.Sleep(500 * time.Millisecond)
		}
		log.Println("CRITICAL: Failed to reconnect")
	} else {
		log.Println("INFO: Connection dropped normally, will not reconnect.")
	}
}
