package httpstream

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
