package factory

import (
	"context"
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	q           amqp.Queue
	consumerTag string
}

func NewQueueMiddleware(queueName string, connectionSettings m.ConnSettings) (m.Middleware, error) {
	qm := new(QueueMiddleware)
	addr := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	var err error

	qm.conn, err = amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	qm.ch, err = qm.conn.Channel()
	if err != nil {
		return nil, err
	}

	qm.consumerTag = "" // dont start consuming

	qm.q, err = qm.ch.QueueDeclare(
		queueName, // name
		true,      // durability
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		amqp.Table{
			amqp.QueueTypeArg: amqp.QueueTypeQuorum,
		},
	)

	return qm, err
}

func (qm *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) (err error) {
	if qm.conn == nil || qm.conn.IsClosed() || qm.ch == nil {
		return m.ErrMessageMiddlewareDisconnected
	}

	qm.consumerTag = "consumerTag" // start consuming

	msgs, err := qm.ch.Consume(
		qm.q.Name,
		qm.consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)

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

func (qm *QueueMiddleware) StopConsuming() {
	if qm.consumerTag == "" {
		return // do nothing
	}

	tag := qm.consumerTag
	qm.consumerTag = ""

	if qm.conn == nil || qm.conn.IsClosed() || qm.ch == nil {
		// La firma no devuelve error, pero en su definición si (preguntar)
		// return m.ErrMessageMiddlewareDisconnected
	}
	err := qm.ch.Cancel(tag, false)

	if err != nil {
		// Ya se habia cerrado el channel? no hago nada?
	}
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
	if qm.conn == nil || qm.conn.IsClosed() || qm.ch == nil {
		return m.ErrMessageMiddlewareDisconnected
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second) // cancel the publish operation if it takes longer than 5 seconds
	defer cancel()

	errPublish := qm.ch.PublishWithContext(
		ctx,
		"",        // use default exchange (routes directly to the queue)
		qm.q.Name, // queue name when using default exchange
		false,     // msg drop message if no queue is bound
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // survives broker restarts
			ContentType:  "text/plain",
			Body:         []byte(msg.Body),
		})

	if errPublish != nil {
		return m.ErrMessageMiddlewareMessage
	}
	return nil
}

func (qm *QueueMiddleware) Close() error {
	var errCh, errConn error

	if qm.ch != nil {
		errCh = qm.ch.Close()
	}
	if qm.conn != nil {
		errConn = qm.conn.Close()
	}

	qm.conn = nil
	qm.ch = nil
	qm.consumerTag = ""

	if errCh != nil || errConn != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}
