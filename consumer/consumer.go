package main

import (
	"context"
	"log"
	"math/rand"
	"net"
	"strconv"

	"github.com/VimleshS/reactive-services/kconfig"
	pb "github.com/VimleshS/reactive-services/protobuf"
	"google.golang.org/grpc"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

//Manager implements the gRPC methods
type Manager struct {
	consumers map[string]*KConsumer
	Group     string
}

func NewManager(group string) *Manager {
	service := Manager{Group: group}
	service.consumers = map[string]*KConsumer{}
	return &service
}

//GetConsumerList retrive list of executing comsumers
func (m *Manager) GetConsumerList(context.Context, *pb.Empty) (*pb.ConsumerList, error) {
	consumers := new(pb.ConsumerList)
	consumers.Items = [](*pb.Consumer){}
	for _, n := range m.consumers {
		consumers.Items = append(consumers.Items, &pb.Consumer{Name: n.name})
	}
	return consumers, nil
}

//RandStringBytes string for name
func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

//StartNewConsumer creates a new consumer
func (m *Manager) StartNewConsumer(context.Context, *pb.Empty) (*pb.Consumer, error) {
	consName := RandStringBytes(5)
	kcon := NewKConsumer(consName, m.Group).Subs(kconfig.Conf.Topic)
	kcon.Consume()
	m.consumers[consName] = kcon
	return &pb.Consumer{Name: consName}, nil
}

//KillConsumer removes consumer
func (m *Manager) KillConsumer(ctx context.Context, c *pb.Consumer) (*pb.Consumer, error) {
	log.Println("KillConsumer " + c.Name)
	kcon, ok := m.consumers[c.Name]
	if ok {
		log.Println("Closing....")
		kcon.Stop()
		log.Println("Caught signal terminating")
	}
	return c, nil
}

func main() {
	group := kconfig.Conf.ConsumerGroup
	log.Println(group)

	serverAddr := net.JoinHostPort(kconfig.Conf.GrpcServer,
		strconv.Itoa(kconfig.Conf.GrpcServerPort))

	lstnr, err := net.Listen("tcp", serverAddr)
	if err != nil {
		log.Fatal("failed to start server:", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKafkaServiceServer(grpcServer, NewManager(group))

	log.Println("starting consumer rpc service on", serverAddr)
	if err := grpcServer.Serve(lstnr); err != nil {
		log.Fatal(err)
	}
}
