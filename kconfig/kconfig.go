package kconfig

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

// Conf a variable holding a values for service
var Conf configuration

type configuration struct {
	KafkaPath           string `json:"kafkaPath"`
	GrpcServerPort      int    `json:"grpcServerPort"`
	MaxMsgLimit         int    `json:"maxMsgLimit"`
	PartitionSize       int    `json:"partitionSize"`
	GrpcServer          string `json:"grpcServer"`
	ConsumerGroup       string `json:"consumerGroup"`
	Topic               string `json:"topic"`
	BootStrapServer     string `json:"bootStrapServer"`
	FlowMonitorPollTime int    `json:"flowMonitorPollTime"`
}

func init() {
	fileData, err := ioutil.ReadFile("../kconfig/conf.json")
	if err != nil {
		panic(err)
	}

	err = json.Unmarshal(fileData, &Conf)
	if err != nil {
		fmt.Println("error:", err)
	}
}

func (c configuration) BootSevers() []string {
	return []string{
		"--bootstrap-server",
		Conf.BootStrapServer,
		"--group",
		Conf.ConsumerGroup,
		"--describe",
	}
}
