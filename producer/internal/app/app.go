package app

import (
	"context"
	"math/rand"
	"os/signal"
	"syscall"
	"time"

	"github.com/arthurshafikov/go-kafka-test/producer/internal/kafka"
	"github.com/arthurshafikov/go-kafka-test/producer/internal/services"
)

func Run() {
	kafka := kafka.NewKafka()

	orderService := services.NewOrderService(kafka)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			kafka.Close()
			return
		default:
		}

		rand.Seed(time.Now().Unix())

		if rand.Intn(2) == 0 {
			orderService.Create()
		}

		if rand.Intn(2) == 0 {
			orderService.Complete()
		}

		if rand.Intn(2) == 0 {
			orderService.Cancelled()
		}

		time.Sleep(time.Second)
	}
}
