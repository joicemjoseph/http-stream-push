package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

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

// Order is
type order struct {
	OrderID string `json:"orderId"`
	UserID  string `json:"userId"`
	Status  string `json:"status"`

	RequestTimeStamp int64  `json:"request_timeStamp"`
	RequestRiderID   string `json:"request_riderId"`

	RequestLocationLng float64 `json:"request_location_lng"`
	RequestLocationLat float64 `json:"request_location_lat"`

	RequestWithLocationUpdate bool   `json:"request_withLocationUpdate"`
	RequestDeviceID           string `json:"request_deviceId"`
	TimeStamp                 int64  `json:"timeStamp"`
}

// Config is struct for reading data from given url.
type flagData struct {
	Name  string `json:"name"`
	Value string `json:"value"`
	Usage string `json:"usage"`
}
type broaker struct {
	reader ReadData
	writer WriteData
}
type etaEvents struct {
	OrderID interface{}
	UserID  interface{}
	Status  string
}
type orderEvents struct {
	OrderID string `json:"orderId"`
	UserID  string `json:"userId"`
	Status  string `json:"status"`
	Request struct {
		TimeStamp int64  `json:"timeStamp"`
		RiderID   string `json:"riderId"`
		Location  struct {
			Lng float64 `json:"lng"`
			Lat float64 `json:"lat"`
		} `json:"location"`
		WithLocationUpdate bool   `json:"withLocationUpdate"`
		DeviceID           string `json:"deviceId"`
	} `json:"request"`
	TimeStamp int64 `json:"timeStamp"`
}
type orderDetailEvents struct {
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

func contains(s *[]string, e *string) bool {
	for _, a := range *s {
		if a == *e {
			return true
		}
	}
	return false
}

// Data is
type Data interface {
	Marshal() ([]byte, error)
	Unmarshal(data []byte) error
}

// For Testing only
func (o *order) Marshal() ([]byte, error) {
	return []byte(""), nil
}

// For Testing only
func (o *order) Unmarshal(data []byte) error {
	return nil
}
func (o *orderDetailEvents) Marshal() ([]byte, error) {
	return []byte(""), nil
}
func (o *orderDetailEvents) Unmarshal(data []byte) error {
	return nil
}
func (o *orderEvents) Marshal() ([]byte, error) {

	orderData := order{
		OrderID:                   o.OrderID,
		UserID:                    o.UserID,
		Status:                    o.Status,
		TimeStamp:                 o.TimeStamp,
		RequestDeviceID:           o.Request.DeviceID,
		RequestLocationLat:        o.Request.Location.Lat,
		RequestLocationLng:        o.Request.Location.Lng,
		RequestRiderID:            o.Request.RiderID,
		RequestTimeStamp:          o.Request.TimeStamp,
		RequestWithLocationUpdate: o.Request.WithLocationUpdate,
	}
	log.Printf("%+v\n\n", orderData)
	return json.Marshal(orderData)
}
func (o *orderEvents) Unmarshal(data []byte) error {
	return json.Unmarshal(data, o)
}
func (e etaEvents) Marshal() ([]byte, error) {
	return []byte(""), nil
}
func (e etaEvents) Unmarshal(data []byte) error {
	return json.Unmarshal(data, e)
}
func getStruct(topic string) (Data, error) {
	name := []string{"order_events", "eta", "order_detail_events", "sample"}
	if !contains(&name, &topic) {
		return nil, errors.New("Not a valid topic")
	}
	if name[0] == topic {
		return new(orderEvents), nil
	}
	if name[1] == topic {
		return new(etaEvents), nil
	}
	if name[2] == topic {
		return new(orderDetailEvents), nil
	}
	// For Testing only
	if name[3] == topic {
		return new(order), nil
	}
	return nil, errors.New("Unable to find topic")
}
func getURL(url *string) (string, error) {
	resp, err := client.Get(*url)
	if err != nil {
		return "", err
	}
	return resp.Status, err
}
