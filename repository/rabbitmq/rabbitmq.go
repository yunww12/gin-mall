package rabbitmq

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SendMessage(ctx context.Context, rabbitMqQueue string, message []byte) error {
	ch, err := GlobalRabbitMQ.Channel()

	if err != nil {
		return err
	}
	defer func(ch *amqp.Channel) {
		err := ch.Close()
		if err != nil {
			return
		}
	}(ch)

	q, err := ch.QueueDeclare(rabbitMqQueue, true, false, false, false, nil)
	if err != nil {
		return err
	}
	err = ch.PublishWithContext(ctx, "", q.Name, false, false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         message},
	)
	return err
}

func ConsumeMessage(ctx context.Context, rabbitMqQueue string) (<-chan amqp.Delivery, error) {
	ch, err := GlobalRabbitMQ.Channel()
	if err != nil {
		fmt.Println("err", err)
		return nil, err
	}
	q, _ := ch.QueueDeclare(rabbitMqQueue, true, false, false, false, nil)
	return ch.Consume(q.Name, "", true, false, false, false, nil)
}
