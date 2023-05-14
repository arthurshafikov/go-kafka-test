package kafka

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type Kafka struct {
	writer *kafka.Writer
}

func NewKafka() *Kafka {
	return &Kafka{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP("localhost:9092"),
			Topic:                  "orders",
			AllowAutoTopicCreation: true,
			Balancer:               &kafka.LeastBytes{},
			RequiredAcks:           kafka.RequireAll,
			BatchTimeout:           time.Second,
		},
	}
}

func (k *Kafka) Write(messageType, message string) {
	err := k.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(messageType),
			Value: []byte(message),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
}

func (k *Kafka) Close() {
	if err := k.writer.Close(); err != nil {
		log.Fatal("failed to close channel:", err)
	}

}
