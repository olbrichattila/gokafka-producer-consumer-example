package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	// Set up configuration
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

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

	// Create Admin client
	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		log.Fatalf("Error creating Kafka admin client: %v", err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Fatalf("Error closing Kafka admin client: %v", err)
		}
	}()

	// Check if the topic already exists
	topic := "test-topic-4"

	topics, err := admin.ListTopics()
	if err != nil {
		log.Fatalf("Error listing topics: %v", err)
	}
	if _, ok := topics[topic]; !ok {
		// Create topic
		topicDetail := &sarama.TopicDetail{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}
		if err := admin.CreateTopic(topic, topicDetail, false); err != nil {
			log.Fatalf("Error creating topic: %v", err)
		}
		log.Printf("Topic %s created successfully", topic)
	} else {
		log.Printf("Topic %s already exists", topic)
	}

	partitions, err := client.Partitions(topic)
	if err != nil {
		log.Fatalf("Error getting partitions for topic: %v", err)
	}

	// Trap SIGINT to gracefully shut down
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// Publish messages
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("Error creating Kafka producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("Error closing Kafka producer: %v", err)
		}
	}()

	go func() {
		reader := bufio.NewReader(os.Stdin)
		partitionIndex := 0
		for {
			msg := &sarama.ProducerMessage{
				Topic:     "test-topic-4",
				Value:     sarama.StringEncoder("Hello Kafka!"),
				Partition: int32(partitions[partitionIndex]),
			}
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Printf("Failed to send message: %v\n", err)
				if err == sarama.ErrLeaderNotAvailable {
					// Retry after a short delay
					log.Println("Retrying message send after 1 second...")
					time.Sleep(1 * time.Second)
					continue
				}
			} else {
				log.Printf("Message sent to partition %d at offset %d\n", partition, offset)
			}
			partitionIndex = (partitionIndex + 1) % len(partitions) // Increment partition index in a round-robin manner

			_, err = reader.ReadByte()
			if err != nil {
				fmt.Println("Error reading input:", err)
				return
			}
			// break
		}
	}()

	<-signals
	log.Println("Shutting down Kafka producer...")
}
