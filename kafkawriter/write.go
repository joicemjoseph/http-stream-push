package kafkawriter

import (
	"context"
	"log"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

// Push data to kafka
func (c *Config) Push(mp *[]byte) error {
	// fmt.Printf("%v", *mp)
	write(c, mp)
	return nil
}

func write(c *Config, mp *[]byte) {
	// log.Output(0, "Incoming data :"+string(*mp))
	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}
	// w := kafka.NewWriter(kafka.WriterConfig{
	// 	Brokers:  []string{"35.229.100.101:9092"},
	// 	Topic:    "sample",
	// 	Balancer: &kafka.LeastBytes{},
	// })
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"35.229.100.101:9092"},
		Topic:    "sample",
		Balancer: &kafka.Hash{},
		Dialer:   dialer,
	})

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: *mp,
		},
	)
	if err != nil {
		log.Output(0, err.Error())
		return
	}
	log.Output(0, "Info: Inserted succesfully")
	w.Close()
}
