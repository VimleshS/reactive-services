package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	pb "github.com/VimleshS/reactive-services/protobuf"
	"google.golang.org/grpc"
)

const (
	server        = "127.0.0.1"
	serverPort    = "50051"
	maxMsgLimit   = 15000
	partitionSize = 3
)

var executingConsumer []string
var increasedConsumer bool

func main() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	serverAddr := net.JoinHostPort(server, serverPort)

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
	fmt.Printf("Caught signal %v: terminating\n", sig)
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
		totalpendingMessage := calculateLag()
		log.Printf("Pending message %d and executing consumers %v \n", totalpendingMessage, executingConsumer)
		if totalpendingMessage > maxMsgLimit && len(executingConsumer) < partitionSize {
			consumer, err := client.StartNewConsumer(context.Background(), &pb.Empty{})
			if err != nil {
				log.Fatal(err)
			}
			log.Printf("%s added consumer to share workload \n", consumer.Name)
			executingConsumer = append(executingConsumer, consumer.Name)
			increasedConsumer = true
		} else if totalpendingMessage <= maxMsgLimit && increasedConsumer {

			fmt.Println(executingConsumer)
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
		// fmt.Println("------------------------")
		// consumerList, err := client.GetConsumerList(context.Background(), &pb.Empty{})
		// for _, c := range consumerList.Items {
		// 	fmt.Println(c.Name)
		// }
		time.Sleep(time.Duration(5 * time.Second))
	}
}

//Not finding a elegant way to calculate offset lag and hence using a shell command.
func calculateLag() int {
	out, err := exec.Command("/home/synerzip/workspace/kafka/kafka_2.11-1.1.0/bin/kafka-consumer-groups.sh",
		"--bootstrap-server", "localhost:9092", "--group", "g1", "--describe").Output()
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
		// fmt.Println(lag)
	}
	return totallag
}
