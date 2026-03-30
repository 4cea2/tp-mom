package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	q    amqp.Queue
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
	return nil
}

func (qm *QueueMiddleware) StopConsuming() {
}

func (qm *QueueMiddleware) Send(msg m.Message) (err error) {
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

	if errCh != nil {
		return errCh
	}
	return errConn
}
