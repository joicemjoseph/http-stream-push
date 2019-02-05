package main

import (
	"flag"
	"log"
	"os"
	"strconv"

	reader "github.com/joicemjoseph/http-stream-push/kafkareader"
	writer "github.com/joicemjoseph/http-stream-push/kafkawriter"
)

func init() {

}
func main() {
	kafkaServerURL, kafkaTopic, kafkaOffset := parse()
	cfg := broaker{reader: reader.Create(kafkaTopic, kafkaServerURL),
		writer: writer.Create(kafkaTopic, kafkaServerURL)}
	structData, err := getStruct(*kafkaTopic)
	if err != nil {
		log.Output(0, err.Error())
		panic(err)
	}
	mp := cfg.reader.Read(kafkaOffset)

	err = structData.Unmarshal(*mp)
	if err != nil {
		log.Output(0, err.Error())
		panic(err)
	}
	converted, err := structData.Marshal()
	if err != nil {
		log.Output(0, err.Error())
		panic(err)
	}
	cfg.writer.Push(&converted)
}

func parse() (*string, *string, *int64) {
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
	return kafkaServerURL, kafkaTopic, kafkaOffset
}
