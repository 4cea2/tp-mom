package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	keys     []string
	exchange string
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

	em.keys = keys
	em.exchange = exchange
	err = em.ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
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
	if em.conn == nil || em.ch == nil {
		// Esto seria preventivo, pero que pasaria si ocurre durante el consumo? (igual que el queue)
		return m.ErrMessageMiddlewareDisconnected
	}
	q, err := em.ch.QueueDeclare(
		"",    // name
		false, // durability
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)

	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for _, key := range em.keys {
		err = em.ch.QueueBind(
			q.Name,      // queue name
			key,         // routing key
			em.exchange, // exchange
			false,
			nil)

		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

	msgs, err := em.ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil)

	if err != nil {
		return m.ErrMessageMiddlewareMessage
	}

	for d := range msgs {
		msg := m.Message{Body: string(d.Body)}

		ack := func() {
			d.Ack(
				false, // only ack this message, not the ones before
			)
		}
		nack := func() {
			d.Nack(
				false, // only nack this message, not the ones before
				true,  // requeue this message instead of discarding it
			)
		}
		callbackFunc(msg, ack, nack)
	}

	return m.ErrMessageMiddlewareDisconnected // msgs is closed
}

func (em *ExchangeMiddleware) StopConsuming() {

}

func (em *ExchangeMiddleware) Send(msg m.Message) (err error) {
	return nil
}

func (em *ExchangeMiddleware) Close() error {
	var errCh, errConn error

	if em.ch != nil {
		errCh = em.ch.Close()
	}
	if em.conn != nil {
		errConn = em.conn.Close()
	}

	if errCh != nil {
		return m.ErrMessageMiddlewareClose
	}
	em.ch = nil

	if errConn != nil {
		return m.ErrMessageMiddlewareClose
	}
	em.conn = nil

	return nil
}
