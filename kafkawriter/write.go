package kafkawriter

import (
	"context"
	"log"

	kafka "github.com/segmentio/kafka-go"
)

// Push data to kafka
func (c *Config) Push(mp *[]byte) error {
	write(c, mp)
	return nil
}

func write(c *Config, mp *[]byte) {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{*c.kafkaURL},
		Topic:    *c.topicName,
		Balancer: &kafka.Hash{},
	})
	// w := kafka.NewWriter(kafka.WriterConfig{
	// 	Brokers:  []string{"35.229.100.101:9092"},
	// 	Topic:    "sample",
	// 	Balancer: &kafka.Hash{},
	// 	Dialer:   dialer,
	// })

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
	defer w.Close()
}

// func parse() (*string, *string) {
// 	f := flag.NewFlagSet("data-to-push", flag.ExitOnError)
// 	url := f.String("out-url", os.Getenv(kafkaOutputURL), "URL to push data to")
// 	topic := f.String("out-topic", os.Getenv(kafkaOutputTopic), "Topic to push data to")
// 	f.Parse(os.Args[1:])

// 	if f.Parsed() && (*url == "" || *topic == "") {
// 		f.PrintDefaults()
// 		os.Exit(1)
// 	}
// 	return url, topic
// }
