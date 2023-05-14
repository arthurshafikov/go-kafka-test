package services

import (
	"fmt"
	"math/rand"
	"time"
)

type KafkaWriter interface {
	Write(messageType, message string)
}

type OrderService struct {
	kafkaWriter KafkaWriter
}

func NewOrderService(kafkaWriter KafkaWriter) *OrderService {
	return &OrderService{
		kafkaWriter: kafkaWriter,
	}
}

func (o *OrderService) Complete() {
	// some code...
	o.kafkaWriter.Write("completed", fmt.Sprintf("Order #%v has been completed!", o.getRandomOrderID()))
}

func (o *OrderService) Create() {
	// some code...
	o.kafkaWriter.Write("created", fmt.Sprintf("Order #%v has been created!", o.getRandomOrderID()))
}

func (o *OrderService) Cancelled() {
	// some code...
	o.kafkaWriter.Write("cancelled", fmt.Sprintf("Order #%v has been cancelled!", o.getRandomOrderID()))
}

func (o *OrderService) getRandomOrderID() int64 {
	randomizer := rand.NewSource(time.Now().Unix())

	return randomizer.Int63()
}
