package kafka

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	reader *kafka.Reader
}

func NewKafka() *Kafka {
	return &Kafka{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{"localhost:9092"},
			Topic:   "orders",
			GroupID: "orderNotifcators",
		}),
	}
}

func (k *Kafka) FetchMessage(ctx context.Context, messages chan<- kafka.Message) error {
	for {
		message, err := k.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return nil
		case messages <- message:
			log.Printf("message sent to a channel messages: %s\n", string(message.Value))
		}
	}
}

func (k *Kafka) CommitMessages(ctx context.Context, messageCommitChan <-chan kafka.Message) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-messageCommitChan:
			if err := k.reader.CommitMessages(ctx, msg); err != nil {
				return err
			}
			log.Printf("message commited: %s\n", string(msg.Value))
		}
	}
}
