package main

import (
	"encoding/json"
	"fmt"
	"log"
	"github.com/IBM/sarama"
	"github.com/gofiber/fiber/v2"
)

type Comment struct {
	Text string `form:"text" json:"text"`
	Type int    `form:"type" json:"type"`  // New Type attribute
}

func main() {
	app := fiber.New()
	api := app.Group("/api/v1")
	api.Post("/comments", createComment)
	log.Fatal(app.Listen(":3000"))
}

func createComment(c *fiber.Ctx) error {
	cmt := new(Comment)
	if err := c.BodyParser(cmt); err != nil {
		c.Status(400).JSON(&fiber.Map{
			"success": false,
			"message": "Unable to parse",
		})
		return err
	}
	
	cmtInBytes, err := json.Marshal(cmt)
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error encoding comment",
		})
		return err
	}
	
	// Determine the topic based on the Comment Type
	topic := getTopicForType(cmt.Type)
	err = PushCommentToQueue(topic, cmtInBytes)
	
	if err != nil {
		c.Status(500).JSON(&fiber.Map{
			"success": false,
			"message": "Error pushing comment",
		})
		return err
	}
	
	c.JSON(&fiber.Map{
		"success": true,
		"message": "Comment pushed successfully",
		"comment": cmt,
	})
	return nil
}

// Function to select topic based on the comment type
func getTopicForType(commentType int) string {
	switch commentType {
	case 1:
		return "topic1"
	case 2:
		return "topic2"
	case 3:
		return "topic3"
	default:
		return "defaultTopic" // Handle undefined types
	}
}

func PushCommentToQueue(topic string, message []byte) error {
	brokerUrl := "localhost:29092"
	producer, err := ConnectProducer(brokerUrl)
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer producer.Close()
	
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(message),
	}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	fmt.Printf("Message is stored in topic (%s)/partition(%d)/offset(%d)\n", topic, partition, offset)
	return nil
}

func ConnectProducer(brokerUrl string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer([]string{brokerUrl}, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
