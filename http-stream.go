package main

import (
	"encoding/json"
	"log"

	reader "github.com/joicemjoseph/http-stream-push/kafkareader"
	writer "github.com/joicemjoseph/http-stream-push/kafkawriter"
)

func main() {
	kafkaReaderURL, kafkaReaderTopic, kafkaReaderOffset, kafkaWriterTopic, kafkaWriterURL := parse()

	cfg := broaker{reader: reader.Create(kafkaReaderTopic, kafkaReaderURL),
		writer: writer.Create(kafkaWriterTopic, kafkaWriterURL)}
	structData, err := getStruct(*kafkaReaderTopic)
	if err != nil {
		log.Output(0, *kafkaReaderTopic+" is "+err.Error())
		panic(err)
	}
	mp := cfg.reader.Read(kafkaReaderOffset)

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
