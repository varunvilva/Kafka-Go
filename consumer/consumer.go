package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	topic := "comments"
	brokers := []string{"localhost:29092"}
	
	// Connect to Kafka consumer
	consumer, err := connectConsumer(brokers)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Println("Error closing consumer:", err)
		}
	}()

	// Get all partitions for the topic
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		panic(err)
	}

	// Set up channels to handle OS signals and messages
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	msgCount := 0
	doneCh := make(chan struct{})

	// Goroutines to process messages for each partition
	for _, partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
		if err != nil {
			panic(err)
		}
		defer func(pc sarama.PartitionConsumer) {
			if err := pc.Close(); err != nil {
				fmt.Println("Error closing partition consumer:", err)
			}
		}(partitionConsumer)

		go func(pc sarama.PartitionConsumer) {
			for {
				select {
				case err := <-pc.Errors():
					fmt.Println("Error:", err)
				case msg := <-pc.Messages():
					msgCount++
					fmt.Printf("Received message Count: %d | Partition: %d | Topic: %s | Message: %s\n", msgCount, msg.Partition, msg.Topic, string(msg.Value))
				case <-sigchan:
					fmt.Println("Interruption detected")
					doneCh <- struct{}{}
					return
				}
			}
		}(partitionConsumer)
	}

	// Wait until done
	<-doneCh
	fmt.Println("Processed", msgCount, "messages")
}

// Function to connect to the Kafka consumer
func connectConsumer(brokers []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
