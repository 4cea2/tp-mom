package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	em := new(ExchangeMiddleware)
	addr := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	var err error

	em.conn, err = amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	em.ch, err = em.conn.Channel()
	if err != nil {
		return nil, err
	}

	err = em.ch.ExchangeDeclare(
		exchange, // name
		"fanout", // type
		false,    // durability
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)

	if err != nil {
		return nil, err
	}

	return em, nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	return nil
}

func (em *ExchangeMiddleware) StopConsuming() {

}

func (em *ExchangeMiddleware) Send(msg m.Message) (err error) {
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	return nil
}
