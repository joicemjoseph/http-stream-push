package httpstream

import (
	"flag"
	"fmt"
	"os"
)

const (
	inURL   = "http://localhost"
	outURL  = "http://35.229.100.101"
	outPort = "8082"
)

// OnlineData is struct for reading data from given url.
type OnlineData struct{}
type flagData struct {
	name  string
	value interface{}
	usage string
}
type receiveData interface {
	// read stream data
	read(*string)
}
type insertData interface {
	// push data
	push(string)
}

func main() {
	var topic, streamServerURL, kafkaServerURL *string

	if *streamServerURL = os.Getenv("topic"); *streamServerURL == "" {
		// parse input stream flag
		fg := flag.NewFlagSet("in", flag.PanicOnError)
		f := &flagData{"in", "http://kredaro.ga:8080", "URL of the incoming stream"}
		streamServerURL = f.getStringFlags(fg)
	}
	if *kafkaServerURL = os.Getenv("topic"); *kafkaServerURL == "" {
		// parse kafka stream server flag
		fg := flag.NewFlagSet("out", flag.PanicOnError)
		f := &flagData{"out", "http://kredaro.ga:8080", "URL of the outgoing stream"}
		kafkaServerURL = f.getStringFlags(fg)
	}
	if *topic = os.Getenv("topic"); *topic == "" {
		// parse topic flag
		fg := flag.NewFlagSet("topic", flag.PanicOnError)
		f := &flagData{"topic", "", "name of the topic to upload the stream"}
		topic = f.getStringFlags(fg)
	}

	sd := OnlineData{}
	data, err := sd.read(streamServerURL)
	if err != nil {
		// do stuff
	}
	fmt.Printf(string(data))
}
func (f *flagData) getStringFlags(flg *flag.FlagSet) *string {
	d := flg.String(f.name, f.value.(string), f.usage)
	flg.Parse([]string{f.name})
	return d
}

func (o *OnlineData) ps(url string) error {
	return nil
}
