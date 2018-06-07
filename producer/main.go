package main

import (
	"fmt"
	"os"

	"github.com/VimleshS/reactive-services/kconfig"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group g1 --describe

func main() {
	// broker := "localhost"
	// topic := "ntest"

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kconfig.Conf.BootStrapServer},
	)

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)

	// Optional delivery channel, if not specified the Producer object's
	// .Events channel is used.
	deliveryChan := make(chan kafka.Event)
	var e kafka.Event

	value := "Hello Reactive Service"

	cnt := 0
	for cnt <= 100000 {
		err = p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &kconfig.Conf.Topic,
				Partition: kafka.PartitionAny},
			Value: []byte(value),
			Headers: []kafka.Header{{
				Key:   "ReactiveService",
				Value: []byte("header values are binary")},
			},
		}, deliveryChan)

		e = <-deliveryChan
		m := e.(*kafka.Message)

		if m.TopicPartition.Error != nil {
			fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		} else {
			fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
				*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		}

		cnt++
	}
	close(deliveryChan)
}
