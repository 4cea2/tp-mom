package factory

import (
	"context"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	*baseMiddleware
	keys     []string
	exchange string
}

func NewExchangeMiddleware(exchange string, keys []string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	em := new(ExchangeMiddleware)
	base, err := newBaseMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}
	em.baseMiddleware = base

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

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if em.conn == nil || em.conn.IsClosed() || em.ch == nil {
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

	em.consumerTag = "consumerTag"
	msgs, err := em.ch.Consume(
		q.Name,
		em.consumerTag,
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

func (em *ExchangeMiddleware) StopConsuming() error {
	if em.consumerTag == "" {
		return nil
	}

	tag := em.consumerTag
	em.consumerTag = ""

	if em.conn == nil || em.conn.IsClosed() || em.ch == nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	err := em.ch.Cancel(tag, false)

	if err != nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (em *ExchangeMiddleware) Send(msg m.Message) error {
	if em.conn == nil || em.conn.IsClosed() || em.ch == nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, key := range em.keys {
		err := em.ch.PublishWithContext(ctx,
			em.exchange, // exchange
			key,         // routing key
			false,       // mandatory
			false,       // immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent, // survives broker restarts
				ContentType:  "text/plain",
				Body:         []byte(msg.Body),
			})
		if err != nil {
			return m.ErrMessageMiddlewareMessage
		}
	}

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

	em.conn = nil
	em.ch = nil
	em.consumerTag = ""

	if errCh != nil || errConn != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
