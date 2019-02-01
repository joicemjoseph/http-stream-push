package main

import (
	"flag"
	"os"

	"github.com/joicemjoseph/http-stream-push/kafka"
)

func init() {

}
func main() {
	// parse flags
	kafkaServerURL := flag.String("url", "", "URL of the kafka server")
	kafkaTopic := flag.String("topic", "", "name of the topic to upload the stream")
	flag.Parse()

	if *kafkaServerURL == "" {
		if o := os.Getenv(kafkaServerENV); o != "" {
			*kafkaServerURL = o
		} else {
			*kafkaServerURL = kafkaDefaultServerURL

		}
	}
	if *kafkaTopic == "" {
		if o := os.Getenv(kafkatopicENV); o != "" {
			*kafkaServerURL = o
		} else {
			*kafkaTopic = kafkaDefaultTopic
		}
	}

	cfg := broaker{handler: kafka.Create(kafkaTopic, kafkaServerURL)}

	cfg.handler.Read()
}
