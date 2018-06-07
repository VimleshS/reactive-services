package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/VimleshS/reactive-services/kconfig"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

//KConsumer implements basic functions to spawn and shut a consumer
type KConsumer struct {
	name     string
	consumer *kafka.Consumer
	stop     chan bool
}

//NewKConsumer ...
// https://github.com/confluentinc/confluent-kafka-go/issues/65
func NewKConsumer(name string, group string) *KConsumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":        kconfig.Conf.BootStrapServer,
		"group.id":                 group,
		"session.timeout.ms":       6000,
		"go.events.channel.enable": true,
		// "go.application.rebalance.enable": true,
		"default.topic.config": kafka.ConfigMap{"auto.offset.reset": "earliest"},
		// "debug":                "cgrp,topic,protocol",
	})

	if err != nil {
		panic(err)
	}

	return &KConsumer{name: name, consumer: c, stop: make(chan bool)}
}

//Subs subscribes to a kafka topic
func (k *KConsumer) Subs(topic string) *KConsumer {
	k.consumer.SubscribeTopics([]string{topic}, nil)
	return k
}

//Consume reads message
func (k *KConsumer) Consume() {
	go func() {
		run := true
		for run == true {
			select {
			case sig := <-k.stop:
				fmt.Printf("Caught signal %v: terminating\n", sig)
				run = false

			case ev := <-k.consumer.Events():
				switch e := ev.(type) {
				case kafka.AssignedPartitions:
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
					k.consumer.Assign(e.Partitions)
				case kafka.RevokedPartitions:
					fmt.Fprintf(os.Stderr, "%% %v\n", e)
					k.consumer.Unassign()
				case *kafka.Message:
					fmt.Printf("%% Consumer %s Message on %s:\n%s\n",
						k.name, e.TopicPartition, string(e.Value))

					time.Sleep(time.Duration(1 * time.Millisecond))
				case kafka.PartitionEOF:
					fmt.Printf("%% Reached %v\n", e)
				case kafka.Error:
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
					run = false
				}
			}
		}
	}()
}

//Stop kills closes consume, note this works properly
//when auto configure option is set to false
func (k *KConsumer) Stop() {
	close(k.stop)
	err := k.consumer.Close()
	if err != nil {
		log.Println(err)
	}
	log.Printf("%% %s Closed. \n", k.name)
}
