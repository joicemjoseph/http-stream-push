package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
)

const (
	jsonContentType = "application/json"
)
const (
	inURL                 = "http://localhost"
	outURL                = "http://35.229.100.101"
	kafkaRestServerPort   = "8082"
	kafkaSchemaServerPort = "8081"
	schemaServerURL       = "http://35.229.100.101"
)

const (
	maxIdleConnections int = 20
	requestTimeout     int = 5
)

var client *http.Client

// Config is struct for reading data from given url.
type Config struct {
	topic *int
}
type flagData struct {
	name  string
	value interface{}
	usage string
}

type initialData struct {
	topicName       *string
	inURL           *string
	outURL          *string
	schemaServerURL *string
}
type receiveData interface {
	// read stream data
	read(*string)
}
type insertData interface {
	// push data
	push(string)
}

func init() {
	client = createHTTPClient()
}
func main() {
	cfg := Config{}
	id := initialData{}
	// var dr = make(chan []byte, 10)
	if err := cfg.init(); err != nil {
		panic(err)
	}
	if err := id.init(); err != nil {
		panic(err)
	}
	cfg.topic, _ = id.getTopicID()
	data, err := cfg.read(id.inURL)
	if err != nil {
		// do stuff
		panic(err)
	}
	// dr <- data
	fmt.Printf(string(data))
}

func (id *initialData) getTopicID() (*int, error) {
	t, _, err := getTopicsAndIDFromServer(id.topicName, id.schemaServerURL)
	if err != nil {
		log.Output(0, "Error:"+err.Error())
		panic(err)
	}

	return t, nil
}

func (c *Config) init() error {

	return nil
}
func (id *initialData) init() error {
	streamServerURL := flag.String("in", "", "URL of the incoming stream")
	kafkaRestServerURL := flag.String("out", "http://localhost", "URL of the outgoing stream")
	topic := flag.String("topic", "topic", "name of the topic to upload the stream")
	flag.Parse()

	id.inURL = streamServerURL
	id.outURL = kafkaRestServerURL
	id.topicName = topic
	return nil
}
