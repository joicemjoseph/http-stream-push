package main

import (
	"flag"
	"os"
	"strconv"

	"github.com/joicemjoseph/http-stream-push/kafka"
)

func init() {

}
func main() {
	// parse flags
	kafkaServerURL := flag.String("url", "", "URL of the kafka server")
	kafkaTopic := flag.String("topic", "", "name of the topic to upload the stream")
	kafkaOffset := flag.Int64("offset", -1, "offset number")
	flag.Parse()

	if *kafkaServerURL == "" {
		if o := os.Getenv(kafkaServerENV); o != "" {
			*kafkaServerURL = o
		} else {
			*kafkaServerURL = kafkaDefaultServerURL

		}
	}
	if *kafkaTopic == "" {
		if o := os.Getenv(kafkaTopicENV); o != "" {
			*kafkaServerURL = o
		} else {
			*kafkaTopic = kafkaDefaultTopic
		}
	}
	if *kafkaOffset < 0 {

		if o, err := strconv.ParseInt(os.Getenv(kafkaOffsetENV), 10, 64); err == nil && o > 0 {
			*kafkaOffset = o
		} else {
			*kafkaOffset = kafkaDefaultOffset
		}
	}

	cfg := broaker{handler: kafka.Create(kafkaTopic, kafkaServerURL)}

	cfg.handler.Read(kafkaOffset)
}
