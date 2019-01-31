package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func createHTTPClient() *http.Client {
	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: maxIdleConnections,
		},
		Timeout: time.Duration(requestTimeout) * time.Second,
	}

	return client
}

func getTopicsAndIDFromServer(topic, url *string) (*int, int, error) {
	// url := SchemaServerPortinstanceIPAddress + ":" + kafkaSchemaServerPort + "/subjects/" + topic + "-value/versions/latest"
	body, statusCode, err := readDataFromURL(*url)
	if err != nil || statusCode != http.StatusOK {
		return nil, statusCode, err
	}
	type valueID struct {
		ID int `json:"id"`
	}
	value := new(valueID)
	err = json.Unmarshal(body, value)
	if err != nil {
		return &value.ID, http.StatusInternalServerError, err
	}
	return &value.ID, http.StatusOK, nil
}
func readDataFromURL(url string) ([]byte, int, error) {
	log.Output(0, "Function: getDataFromURL")
	resp, err := client.Get(url)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}
	log.Output(0, "request to url: "+url+" responded with "+resp.Status)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, http.StatusBadRequest, err
	}

	return body, resp.StatusCode, nil
}

// Legacy purpose

// func (f *flagData) getStringFlags(flg *flag.FlagSet) *string {
// 	d := flg.String(f.name, f.value.(string), f.usage)
// 	// fmt.Printf("data: %v\n", os.Args[1:])
// 	flg.Parse([]string{"in"})
// 	if flg.Parsed() {
// 		log.Output(0, "From Parsed: "+*d)
// 		return d
// 	}
// 	return nil
// }

// Snippet for environment check.
// if s := os.Getenv("in"); s == "" {
// 	// parse input stream flag
// 	// fg := flag.NewFlagSet("in", flag.PanicOnError)
// 	// f := flagData{"in", "", "URL of the incoming stream"}
// 	// streamServerURL = f.getStringFlags(fg)
// 	// log.Output(0, *streamServerURL)
// 	if *streamServerURL == "" {
// 		*streamServerURL = inURL
// 	} else {
// 		streamServerURL = &s
// 	}
// }
// if s := os.Getenv("out"); s == "" {
// 	// parse kafka stream server flag
// 	// fg := flag.NewFlagSet("out", flag.PanicOnError)
// 	// f := flagData{"out", "http://localhost", "URL of the outgoing stream"}
// 	// kafkaRestServerURL = f.getStringFlags(fg)
// 	if *kafkaRestServerURL == "" {
// 		*kafkaRestServerURL = outURL + ":" + kafkaRestServerPort
// 	} else {
// 		kafkaRestServerURL = &s
// 	}
// }
// if s := os.Getenv("topic"); s == "" {
// 	// parse topic flag
// 	// fg := flag.NewFlagSet("topic", flag.PanicOnError)
// 	// f := flagData{"topic", "topic", "name of the topic to upload the stream"}
// 	// topic = f.getStringFlags(fg)
// 	if *topic == "" {
// 		// topic cannot be null
// 		return errors.New("Topic can't be null")
// 	} else {
// 		s =
// 	}
// }
