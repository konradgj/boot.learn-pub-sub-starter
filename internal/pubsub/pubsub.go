package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int
type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

const (
	Durable = iota
	Transient
)

func encode[T any](val T) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(val); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decode[T any](data []byte) (T, error) {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	var val T
	if err := dec.Decode(&val); err != nil {
		return val, err
	}
	return val, nil
}

func jsonDecode[T any](data []byte) (T, error) {
	var res T
	err := json.Unmarshal(data, &res)
	if err != nil {
		return res, err
	}
	return res, err
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := encode(val)
	if err != nil {
		return fmt.Errorf("could not encode val: %v", err)
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        data,
	})

	return nil
}

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("could not marshal val: %w", err)
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})

	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		decode[T],
	)
	if err != nil {
		return err
	}
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		jsonDecode[T],
	)
	if err != nil {
		return err
	}
	return nil
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
) error {
	ch, _, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not bind queue: %w", err)
	}

	messages, err := ch.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume channel: %w", err)
	}

	go func() {
		defer ch.Close()
		for msg := range messages {
			data, err := unmarshaller(msg.Body)
			if err != nil {
				log.Printf("could not umarshal message: %v\n", err)
				continue
			}

			switch handler(data) {
			case Ack:
				if err := msg.Ack(false); err != nil {
					log.Printf("failed to Ack message: %v\n", err)
				}
			case NackRequeue:
				if err := msg.Nack(false, true); err != nil {
					log.Printf("failed to NackRequeue message: %v\n", err)
				}
			case NackDiscard:
				if err := msg.Nack(false, false); err != nil {
					log.Printf("failed to NackDiscard message: %v\n", err)
				}
			}

		}
	}()

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %w", err)
	}

	queue, err := ch.QueueDeclare(queueName,
		queueType == Durable,
		queueType == Transient,
		queueType == Transient,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)
	if err != nil {
		return ch, amqp.Queue{}, fmt.Errorf("coud not create queue: %w", err)
	}

	err = ch.QueueBind(queue.Name, key, exchange, false, nil)
	if err != nil {
		return ch, queue, fmt.Errorf("could not bind queue: %w", err)
	}

	return ch, queue, nil
}
