package httpstream

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
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
	initData initialData
	topic    *int
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
	if err := cfg.init(); err != nil {
		panic(err)
	}

	data, err := cfg.read(cfg.initData.inURL)
	if err != nil {
		// do stuff
		panic(err)
	}
	fmt.Printf(string(data))
}
func (f *flagData) getStringFlags(flg *flag.FlagSet) (*string, error) {
	d := flg.String(f.name, f.value.(string), f.usage)
	flg.Parse([]string{f.name})
	return d, nil
}

func (c *Config) setTopicID(topic *string) {
	t, _, err := getTopicsAndIDFromServer(topic, c.initData.schemaServerURL)
	if err != nil {
		log.Output(0, err.Error())
		panic(err)
	}
	c.topic = t
}

func (c *Config) init() error {
	return c.initData.init()
}
func (id *initialData) init() error {
	var topic, streamServerURL, kafkaRestServerURL *string
	if *streamServerURL = os.Getenv("in"); *streamServerURL == "" {
		// parse input stream flag
		fg := flag.NewFlagSet("in", flag.PanicOnError)
		f := &flagData{"in", "http://kredaro.ga:8080", "URL of the incoming stream"}
		streamServerURL, err := f.getStringFlags(fg)
		if err != nil {
			*streamServerURL = inURL
		}
	}
	if *kafkaRestServerURL = os.Getenv("out"); *kafkaRestServerURL == "" {
		// parse kafka stream server flag
		fg := flag.NewFlagSet("out", flag.PanicOnError)
		f := &flagData{"out", "http://kredaro.ga:8080", "URL of the outgoing stream"}
		kafkaRestServerURL, err := f.getStringFlags(fg)
		if err != nil {
			*kafkaRestServerURL = outURL + ":" + kafkaRestServerPort
		}
	}
	if *topic = os.Getenv("topic"); *topic == "" {
		// parse topic flag
		fg := flag.NewFlagSet("topic", flag.PanicOnError)
		f := &flagData{"topic", "", "name of the topic to upload the stream"}
		topic, err := f.getStringFlags(fg)
		if err != nil {
			// topic cannot be null
			log.Output(0, "Error: "+err.Error())
			log.Output(0, "Topic: "+*topic)
			return err
		}
	}
	id.inURL = streamServerURL
	id.outURL = kafkaRestServerURL
	id.topicName = topic
	return nil
}
