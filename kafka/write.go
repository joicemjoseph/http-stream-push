package kafka

import (
	"encoding/json"
	"log"
)

// Push data to kafka
func (c *Config) Push() error {

	return nil
}

// Has to change data of json.RawMessage to structure channel.
func postHTTPWithData(url string, data json.RawMessage) ([]byte, int, error) {
	log.Output(0, "Function: postHTTPWithData")

	return nil, 0, nil

}
