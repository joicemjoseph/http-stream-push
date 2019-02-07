package main

import (
	"encoding/json"
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

	err = json.Unmarshal(*mp, structData)
	if err != nil {
		log.Output(0, err.Error())
		panic(err)
	}
	converted, err := json.Marshal(structData)
	if err != nil {
		log.Output(0, err.Error())
		panic(err)
	}
	log.Print("Info: ", string(converted))
	cfg.writer.Push(&converted)
}

func parse() (*string, *string, *int64) {
	// parse flags

	offset, _ := strconv.ParseInt(os.Getenv(kafkaOffsetENV), 10, 64)

	read := flag.NewFlagSet("Read data from Kafka", flag.ExitOnError)
	kafkaServerURL := read.String("in-url", os.Getenv(kafkaServerENV), "URL of the kafka server")
	kafkaTopic := read.String("in-topic", os.Getenv(kafkaTopicENV), "name of the topic to upload the stream")
	kafkaOffset := read.Int64("offset", offset, "offset number")
	read.Parse(os.Args[1:])

	if *kafkaServerURL == "" {
		*kafkaServerURL = kafkaDefaultServerURL
	}
	if *kafkaTopic == "" {
		*kafkaTopic = kafkaDefaultTopic
	}
	if *kafkaOffset < 0 {
		*kafkaOffset = kafkaDefaultOffset
	}
	if read.Parsed() && (*kafkaServerURL == "" || *kafkaTopic == "" || *kafkaOffset < 0) {
		flag.PrintDefaults()
		os.Exit(1)
	}
	return kafkaServerURL, kafkaTopic, kafkaOffset
}
