package kafkawriter

import (
	"context"

	kafka "github.com/segmentio/kafka-go"
)

// Push data to kafka
func (c *Config) Push(mp *[]byte) error {
	return write(c, mp)

}

func write(c *Config, mp *[]byte) error {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{*c.kafkaURL},
		Topic:    *c.topicName,
		Balancer: &kafka.Hash{},
	})

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Value: *mp,
		},
	)
	if err != nil {
		return err
	}
	defer w.Close()
	return nil
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
