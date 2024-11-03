package queue

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQConfig struct {
	URL       string
	TopicName string
	Timeout   time.Time
}

func newRabbitConn(cfg RabbitMQConfig) (rc *RabbitConnection, err error) {
	rc.cfg = cfg
	rc.conn, err = amqp.Dial(cfg.URL)

	return rc, err
}

type RabbitConnection struct {
	cfg RabbitMQConfig
	// Driver de conexão com rabbit
	conn *amqp.Connection
}

func (rc *RabbitConnection) Publish(msg []byte) error {
	// Abre canal para envio de mensagens
	c, err := rc.conn.Channel()
	if err != nil {
		return err
	}

	mp := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         msg,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	return c.PublishWithContext(
		ctx,
		"",
		rc.cfg.TopicName,
		false,
		false,
		mp,
	)
}

func (rc *RabbitConnection) Consume(channelDto chan<- QueueDto) error {
	channel, err := rc.conn.Channel()
	if err != nil {
		return err
	}

	queue, err := channel.QueueDeclare(
		rc.cfg.TopicName,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	msgs, err := channel.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)

	for delivery := range msgs {
		dto := QueueDto{}
		// Hidrata o a instacia do dto
		dto.Unmarshal(delivery.Body)
		channelDto <- dto
	}
	return nil
}
