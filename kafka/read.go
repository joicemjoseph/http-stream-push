package kafka

import (
	"context"
	"fmt"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

type input struct {
	ID string `json:"_id"`
}

// Read data from kafka
func (c *Config) Read() {
	reader(c)
}
func reader(c *Config) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{*c.kafkaURL},
		Topic:     *c.topicName,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(0)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Output(0, "Error: "+err.Error())
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}
