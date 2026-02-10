package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/orasis-holding/pricing-go-swiss-army-lib/kafka"
	"github.com/orasis-holding/pricing-go-swiss-army-lib/mongo"
	"github.com/prometheus/client_golang/prometheus"
	"go.mongodb.org/mongo-driver/bson"
)



type MyProcessor struct {
	Incoming chan string
	Client *mongo.ConnInfo
}

func (p *MyProcessor) ProcessRecord(ctx context.Context, record kafka.Record) error {
	log.Default().Println("incoming message from ProcessRecord")
	log.Default().Printf("data -> topic: %s, partition: %d, offset: %d\n", record.GetTopic(), record.GetPartition(), record.GetOffset())

	key := record.GetKey()
	value := record.GetValue()
	h := record.GetHeaders()
	t := record.GetTimestamp()

	// implement repiping here
	log.Default().Printf("[ProcessRecord] timestamp: %s, key: %s, value : %s\n", t.GoString(), string(key), string(value))

	// only when needed
	for _, header := range h {
		log.Default().Printf("Header: %s = %s", header.GetKey(), string(header.GetValue()))
	}

	_, err := p.Client.InsertOne(ctx, "messages", bson.M{"author" : string(key), "message" : string(value), "timestamp" : t.GoString()})
	if err != nil {
		log.Default().Println("error : could not insert message into db: ", err)
	}

	return nil
}

func main() {
	log.Default().Println("starting consumer")

	ctx := context.Background()

	// Get Kafka brokers from environment variable, default to redpanda service
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	mongoConnString := os.Getenv("MONGOURI")
	if kafkaBrokers == "" || mongoConnString == "" {
		// kafkaBrokers = "redpanda.redpanda.svc.cluster.local:9092"
		log.Fatal("could not get env vars")
	}

	log.Default().Println("address: ", kafkaBrokers)

	// Split by comma if multiple brokers are provided, and trim whitespace
	brokerList := strings.Split(kafkaBrokers, ",")
	brokers := make([]string, 0, len(brokerList))
	for _, broker := range brokerList {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}

	topics := []string{
		"test-topic",
	}

	log.Default().Println("brokers: ", brokerList, "topic: ", topics)

	cfg := kafka.Config{
		Brokers:       brokers,
		Topics:        topics,
		ConsumerGroup: "group-1"}

	processor := &MyProcessor{}

	metrics := kafka.NewCommonMetrics(prometheus.NewRegistry(), "trace")

	timeout, cancel := context.WithTimeout(ctx, time.Second*30)
	defer cancel()

	log.Default().Println("connecting to: ", mongoConnString)

	client, err := mongo.NewClient(ctx, mongoConnString, "messages_db", "messages", "tester")
	if err != nil {
		panic(err)
	}

	defer client.Close(ctx)

	log.Default().Println("successfully connected")


	processor.Client = client

	consumer, err := kafka.NewGroupConsumer(
		timeout,
		nil,
		kafka.WithConsumerConfig(cfg),
		kafka.WithConsumerProcessor(processor),
		kafka.WithConsumerMetrics(metrics),
	)
	if err != nil {
		log.Default().Println("error starting consumer group: ", err)
		return
	}

	log.Default().Println("consumer started")

	

	if err := consumer.Run(ctx); err != nil {
		fmt.Println("error: ", err)
	}

}
