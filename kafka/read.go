package kafka

import (
	"context"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

// Read data from kafka
func (c *Config) Read(offset *int64) {
	reader(c, offset)
}
func reader(c *Config, offset *int64) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{*c.kafkaURL},
		Topic:     *c.topicName,
		Partition: 0,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
	})
	r.SetOffset(*offset)

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Output(0, "Error: "+err.Error())
			break
		}
		// data, _ := json.Marshal(m)
		log.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	}

	r.Close()
}
