package kafka

import (
	"net/http"
	"time"
)

// Config for confguration
type Config struct {
	topicName *string
	kafkaURL  *string
}

const (
	jsonContentType      = "application/json"
	avroKafkaContentType = "application/vnd.kafka.avro.v2+json"
)

const (
	maxIdleConnections int = 20
	requestTimeout     int = 5
)

// var client *http.Client

func setClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxIdleConnections,
		},
		Timeout: time.Duration(requestTimeout) * time.Second,
	}
	return client
}

// Create a new instance of the same.
func Create(topic, kafkaServerURL *string) *Config {

	config := Config{}
	config.init(topic, kafkaServerURL)
	return &config
}

func (c *Config) init(topic, kafkaServerURL *string) error {

	c.topicName = topic
	c.kafkaURL = kafkaServerURL
	return nil
}
