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

	// Start consuming messages from the specified topic and partition
	partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			fmt.Println("Error closing partition consumer:", err)
		}
	}()
	fmt.Println("Consumer started")

	// Set up channels to handle OS signals and messages
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	msgCount := 0
	doneCh := make(chan struct{})

	// Goroutine to process messages
	go func() {
		for {
			select {
			case err := <-partitionConsumer.Errors():
				fmt.Println("Error:", err)
			case msg := <-partitionConsumer.Messages():
				msgCount++
				fmt.Printf("Received message Count: %d | Topic: %s | Message: %s\n", msgCount, msg.Topic, string(msg.Value))
			case <-sigchan:
				fmt.Println("Interruption detected")
				doneCh <- struct{}{}
				return
			}
		}
	}()

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
