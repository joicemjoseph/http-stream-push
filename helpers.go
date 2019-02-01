package main

import "net/http"

const (
	kafkaDefaultServerURL = "http://localhost"
	kafkaDefaultTopic     = "test"
	kafkaDefaultOffset    = 0
)
const (
	kafkaServerENV = "KAFKASERVERURL"
	kafkaTopicENV  = "KAFKATOPIC"
	kafkaOffsetENV = "KAFKAOFFSET"
)

var client *http.Client

// Config is struct for reading data from given url.
type flagData struct {
	name  string
	value interface{}
	usage string
}
type broaker struct {
	handler HandleData
}

// HandleData is
type HandleData interface {

	// read stream data
	Read(*int64)
	// push data to
	Push() error
}
