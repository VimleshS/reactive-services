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
			log.Printf("%s added consumer to share workload \n", consumer.Name)
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
					panic(err)
				}
				executingConsumer = executingConsumer[1:]
				log.Printf("%s removed consumer \n", consumer.Name)
			}
		}
		time.Sleep(time.Duration(5 * time.Second))
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

//Not finding a elegant way to calculate offset lag and hence using a shell command.
func offsetLagFromKafkaConsumerGrp() int {
	kafkaShPath := path.Join(kconfig.Conf.KafkaPath, "kafka-consumer-groups.sh")
	// out, err := exec.Command("/home/synerzip/workspace/kafka/kafka_2.11-1.1.0/bin/kafka-consumer-groups.sh",
	// 	"--bootstrap-server", "localhost:9092", "--group", "g1", "--describe").Output()

	// args := []string{"--bootstrap-server", "localhost:9092", "--group", "g1", "--describe"}
	out, err := exec.Command(kafkaShPath, kconfig.Conf.BootSevers()...).Output()
	if err != nil {
		log.Fatal(err)
	}
	result := string(out)
	_t := strings.Split(result, "\n")

	totallag := 0
	//first two lines include \n and header
	for _, t := range _t[2:] {
		if strings.TrimSpace(t) == "" {
			continue
		}
		lag, err := strconv.Atoi(strings.TrimSpace(t[59:75]))
		totallag += lag
		if err != nil {
			fmt.Println(t)
			fmt.Println(err)
		}
	}
	return totallag
}
