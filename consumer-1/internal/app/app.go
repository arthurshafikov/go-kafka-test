package app

import (
	"context"
	"log"

	"github.com/arthurshafikov/go-kafka-test/consumer-1/internal/kafka"
	kafkago "github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

func Run() {
	kafkaReader := kafka.NewKafka()

	messages := make(chan kafkago.Message, 1000)
	messageCommitChan := make(chan kafkago.Message, 1000)

	g, ctx := errgroup.WithContext(context.Background())

	g.Go(func() error {
		return kafkaReader.FetchMessage(ctx, messages)
	})

	g.Go(func() error {
		return kafkaReader.CommitMessages(ctx, messageCommitChan)
	})

	g.Go(func() error {
		// process messages
		for msg := range messages {
			messageCommitChan <- msg
		}

		return nil
	})

	if err := g.Wait(); err != nil {
		log.Fatalln(err)
	}
}
