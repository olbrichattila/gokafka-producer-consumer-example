package main

import (
	"context"
	"log"
	"os"
	"os/signal"

	"github.com/IBM/sarama"
)

func main() {
	// Set up configuration
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Initialize Kafka client
	client, err := sarama.NewClient([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka client: %v", err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Fatalf("Error closing Kafka client: %v", err)
		}
	}()

	// Initialize consumer group
	consumerGroup, err := sarama.NewConsumerGroupFromClient("group-name", client)
	if err != nil {
		log.Fatalf("Error creating Kafka consumer group: %v", err)
	}
	defer func() {
		if err := consumerGroup.Close(); err != nil {
			log.Fatalf("Error closing Kafka consumer group: %v", err)
		}
	}()

	// Consume messages
	go func() {
		ctx := context.Background()
		for {
			// Consume messages from all partitions of the topic
			topics := []string{"test-topic-4"} // Replace "test-topic" with your topic name
			handler := ConsumerHandler{}

			if err := consumerGroup.Consume(ctx, topics, handler); err != nil {
				log.Fatalf("Error consuming messages: %v", err)
			}
		}
	}()

	// Trap SIGINT to gracefully shut down
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	<-signals
	log.Println("Shutting down Kafka consumer...")
}

// ConsumerHandler implements sarama.ConsumerGroupHandler interface
type ConsumerHandler struct{}

// Setup is called when the consumer group is initially created
func (h ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

// Cleanup is called when the consumer group is terminated
func (h ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim is called when the consumer group has messages to consume
func (h ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: topic=%s, partition=%d, offset=%d, value=%s\n",
			message.Topic, message.Partition, message.Offset, string(message.Value))
		session.MarkMessage(message, "")
	}
	return nil
}
