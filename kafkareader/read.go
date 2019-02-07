package kafkareader

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip" // ETA is gzipped.
)

// Read data from kafka
func (c *Config) Read(offset *int64) (*[]byte, error) {
	return reader(c, offset)
}
func reader(c *Config, offset *int64) (*[]byte, error) {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{*c.kafkaURL},
		Topic:     *c.topicName,
		Partition: 0,
		MinBytes:  0,   // 10KB
		MaxBytes:  1e6, // 1MB

	})
	r.SetOffset(*offset)
	// for {
	m, err := r.ReadMessage(context.Background())
	if err != nil {
		return nil, err

	}
	// data, _ := json.Marshal(m)
	r.Close()
	return &m.Value, nil
}
