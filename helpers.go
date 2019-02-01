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
	reader ReadData
	writer WriteData
}

// ReadData is to read data.
// It can be either file, db or kafka
type ReadData interface {

	// read stream data
	Read(*int64) *[]byte
}

// WriteData to write data.
// It can be file, DB or kafka
type WriteData interface {
	// push data to
	Push(*[]byte) error
}
