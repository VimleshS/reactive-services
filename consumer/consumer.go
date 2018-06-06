package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"

	pb "github.com/VimleshS/reactive-services/protobuf"
	"google.golang.org/grpc"
)

const port = ":50051"
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

type Manager struct {
	consumers map[string]*KConsumer
	Group     string
}

func (m *Manager) GetConsumerList(context.Context, *pb.Empty) (*pb.ConsumerList, error) {
	consumers := new(pb.ConsumerList)
	consumers.Items = [](*pb.Consumer){}
	for _, n := range m.consumers {
		consumers.Items = append(consumers.Items, &pb.Consumer{Name: n.name})
	}
	return consumers, nil
}

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func (m *Manager) StartNewConsumer(context.Context, *pb.Empty) (*pb.Consumer, error) {
	topic := "ntest"
	consName := RandStringBytes(5)
	kcon := NewKConsumer(consName, m.Group).Subs(topic)
	kcon.Consume()
	m.consumers[consName] = kcon
	return &pb.Consumer{Name: consName}, nil
}

func (m *Manager) KillConsumer(ctx context.Context, c *pb.Consumer) (*pb.Consumer, error) {
	fmt.Println("KillConsumer " + c.Name)
	kcon, ok := m.consumers[c.Name]
	if ok {
		fmt.Println("Closing....")
		kcon.Stop()
		fmt.Println("Caught signal terminating")
	}
	return c, nil
}

func main() {
	if len(os.Args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}
	group := os.Args[1]
	fmt.Println(group)

	lstnr, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("failed to start server:", err)
	}

	service := Manager{Group: group}
	service.consumers = map[string]*KConsumer{}
	grpcServer := grpc.NewServer()
	pb.RegisterKafkaServiceServer(grpcServer, &service)

	// start service's server
	log.Println("starting consumer rpc service on", port)
	if err := grpcServer.Serve(lstnr); err != nil {
		log.Fatal(err)
	}

}
