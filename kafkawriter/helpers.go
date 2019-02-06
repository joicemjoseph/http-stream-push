package kafkawriter

// Config for confguration
type Config struct {
	topicName *string
	kafkaURL  *string
}

const (
	kafkaOutputURL   = "KAFKAOUTPUTURL"
	kafkaOutputTopic = "KAFKAOUTPUTTOPIC"
)

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
