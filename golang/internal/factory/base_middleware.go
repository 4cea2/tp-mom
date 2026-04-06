package factory

import (
	"fmt"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type baseMiddleware struct {
	conn        *amqp.Connection
	ch          *amqp.Channel
	consumerTag string
}

func newBaseMiddleware(connectionSettings m.ConnSettings) (*baseMiddleware, error) {
	bm := new(baseMiddleware)

	addr := fmt.Sprintf("amqp://guest:guest@%s:%d/", connectionSettings.Hostname, connectionSettings.Port)
	var err error

	bm.conn, err = amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	bm.ch, err = bm.conn.Channel()
	if err != nil {
		return nil, err
	}

	bm.consumerTag = ""

	return bm, nil
}

func (bm *baseMiddleware) isDisconnected() bool {
	return bm.conn == nil || bm.conn.IsClosed() || bm.ch == nil
}
