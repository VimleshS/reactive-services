package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/VimleshS/reactive-services/kconfig"
	pb "github.com/VimleshS/reactive-services/protobuf"
	"google.golang.org/grpc"
)

var executingConsumer []string
var increasedConsumer bool

func main() {
	fmt.Println(kconfig.Conf)
	if kconfig.Conf.KafkaPath == "" {
		log.Fatal("kafkaPath not found")
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	serverAddr := net.JoinHostPort(kconfig.Conf.GrpcServer,
		strconv.Itoa(kconfig.Conf.GrpcServerPort))

	// setup insecure connection
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewKafkaServiceClient(conn)
	kickofConsumer(client)
	go monitorKafka(client)
	sig := <-sigchan
	log.Printf("Caught signal %v: terminating\n", sig)
}

func kickofConsumer(client pb.KafkaServiceClient) {
	consumer, err := client.StartNewConsumer(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatal(err)
	}
	executingConsumer = append(executingConsumer, consumer.Name)
	log.Println("Started default consumer")
}
func monitorKafka(client pb.KafkaServiceClient) {
	for {
		totalpendingMessage := offsetLagFromKafkaConsumerGrp()
		log.Printf("Pending message %d and executing consumers %v \n",
			totalpendingMessage, executingConsumer)

		if pendingMsgExceedLimit(totalpendingMessage) &&
			consumerBelowThreshhold(len(executingConsumer)) {
			consumer, err := client.StartNewConsumer(context.Background(), &pb.Empty{})
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("Consumer '%s' is added to share workload \n", consumer.Name)
			executingConsumer = append(executingConsumer, consumer.Name)
			increasedConsumer = true
		} else if msgInLimit(totalpendingMessage) && increasedConsumer {
			log.Printf("Consumers %v \n", executingConsumer)
			//remove consumer
			if len(executingConsumer) >= 2 {
				toRemove := executingConsumer[0]
				log.Printf("%s About to remove consumer \n", toRemove)
				consumer, err := client.KillConsumer(context.Background(), &pb.Consumer{Name: toRemove})
				if err != nil {
					log.Fatal(err)
				}
				executingConsumer = executingConsumer[1:]
				log.Printf("%s removed consumer \n", consumer.Name)
			}
		}
		time.Sleep(time.Duration(
			time.Duration(kconfig.Conf.FlowMonitorPollTime) * time.Second),
		)
	}
}
func pendingMsgExceedLimit(pendingMsg int) bool {
	return pendingMsg > kconfig.Conf.MaxMsgLimit
}

func msgInLimit(pendingMsg int) bool {
	return pendingMsg <= kconfig.Conf.MaxMsgLimit
}

func consumerBelowThreshhold(nosOfConsumers int) bool {
	return nosOfConsumers < kconfig.Conf.PartitionSize
}

//TODO: Replace with better algorithm. Not finding a elegant way to
//calculate offset lag and hence using a shell command.
func offsetLagFromKafkaConsumerGrp() int {
	kafkaShPath := path.Join(kconfig.Conf.KafkaPath, "kafka-consumer-groups.sh")
	out, err := exec.Command(kafkaShPath, kconfig.Conf.BootSevers()...).Output()
	if err != nil {
		log.Fatal(err)
	}
	result := string(out)
	_t := strings.Split(result, "\n")

	//first two lines include \n and header
	totallag := 0
	for _, t := range _t[2:] {
		if strings.TrimSpace(t) == "" {
			continue
		}
		seps := strings.Split(t, " ")
		_tmp := []string{}
		for _, sep := range seps {
			if strings.TrimSpace(sep) != "" {
				_tmp = append(_tmp, sep)
			}
		}

		lag, err := strconv.Atoi(strings.TrimSpace(_tmp[4]))
		totallag += lag
		if err != nil {
			log.Fatalln(err)
		}
	}
	return totallag
}
