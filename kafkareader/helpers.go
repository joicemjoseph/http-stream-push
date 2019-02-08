package kafkareader

// Config for confguration
type Config struct {
	topicName *string
	kafkaURL  *string
}

// KafkaResult stores information from kafka.
type KafkaResult struct {
	Message   []byte
	Err       error
	Counter   int64
	Partition int
}

// Create a new instance of the same.
func Create(topic, kafkaServerURL *string) *Config {

	config := Config{}
	config.init(topic, kafkaServerURL)
	return &config
}

func (c *Config) init(topic, kafkaServerURL *string) {

	c.topicName = topic
	c.kafkaURL = kafkaServerURL
}
