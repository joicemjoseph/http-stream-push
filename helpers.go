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

type orderDetailEvents struct {
}

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
	type orderDetails struct {
		ID             string `json:"_id"`
		PickupLocation struct {
			LocationType []string `json:"locationType"`
			Address      string   `json:"address"`
			Lat          float64  `json:"lat"`
			Lng          float64  `json:"lng"`
			CurrLat      float64  `json:"currLat"`
			CurrLng      float64  `json:"currLng"`
			SetLat       float64  `json:"setLat"`
			SetLng       float64  `json:"setLng"`
		} `json:"pickupLocation"`
		DropLocation struct {
			LocationType []string `json:"locationType"`
			Address      string   `json:"address"`
			Lat          float64  `json:"lat"`
			Lng          float64  `json:"lng"`
			SetLat       float64  `json:"setLat"`
			SetLng       float64  `json:"setLng"`
		} `json:"dropLocation"`
		Collected struct {
			Cash       int `json:"cash"`
			Paytm      int `json:"paytm"`
			Rapido     int `json:"rapido"`
			Wallet     int `json:"wallet"`
			Cwallet    int `json:"cwallet"`
			Mobikwik   int `json:"mobikwik"`
			Freecharge int `json:"freecharge"`
			Lazypay    int `json:"lazypay"`
		} `json:"collected"`
		Feedback struct {
			CustomerRated       bool          `json:"customerRated"`
			CustomerRateService []interface{} `json:"customerRateService"`
			ReviewStatus        bool          `json:"reviewStatus"`
		} `json:"feedback"`
		RequestID         string        `json:"requestId"`
		UserFirstBooking  bool          `json:"userFirstBooking"`
		FakeGps           bool          `json:"fakeGps"`
		PaymentStatus     string        `json:"paymentStatus"`
		OrderType         string        `json:"orderType"`
		ClientID          string        `json:"clientId"`
		RideTime          float64       `json:"rideTime"`
		Discount          int           `json:"discount"`
		SubTotal          int           `json:"subTotal"`
		Amount            int           `json:"amount"`
		CancelFee         int           `json:"cancelFee"`
		PrevDue           int           `json:"prevDue"`
		PrevDueIds        []interface{} `json:"prevDueIds"`
		TaxPercent        int           `json:"taxPercent"`
		TaxAmount         int           `json:"taxAmount"`
		CashBack          int           `json:"cashBack"`
		UserCashbackType  interface{}   `json:"userCashbackType"`
		HailingVerified   bool          `json:"hailingVerified"`
		Status            string        `json:"status"`
		PickupClustersAll []string      `json:"pickupClustersAll"`
		DropClustersAll   []string      `json:"dropClustersAll"`
		PrevRiders        []interface{} `json:"prevRiders"`
		PaymentType       string        `json:"paymentType"`
		Customer          struct {
			ID        string `json:"_id"`
			Mobile    string `json:"mobile"`
			Email     string `json:"email"`
			FirstName string `json:"firstName"`
			Gender    int    `json:"gender"`
			LastName  string `json:"lastName"`
		} `json:"customer"`
		ServiceObj struct {
			ServiceID   string        `json:"serviceId"`
			MinimumFare int           `json:"minimumFare"`
			Extra       int           `json:"extra"`
			PricePerKm  int           `json:"pricePerKm"`
			Rule        string        `json:"rule"`
			BaseFare    int           `json:"baseFare"`
			PriceWithKm []interface{} `json:"priceWithKm"`
			PriceByKm   []struct {
				Price int `json:"price"`
				Kms   int `json:"kms"`
			} `json:"priceByKm"`
			City             string `json:"city"`
			CityID           string `json:"cityId"`
			CityRadius       int    `json:"cityRadius"`
			CancelCharge     int    `json:"cancelCharge"`
			Service          string `json:"service"`
			PlatformCharges  int    `json:"platformCharges"`
			InsuranceCharges int    `json:"insuranceCharges"`
			Tax              struct {
				Igst struct {
					Percent   int    `json:"percent"`
					AppliedOn string `json:"appliedOn"`
				} `json:"igst"`
				Sgst struct {
					Percent   int    `json:"percent"`
					AppliedOn string `json:"appliedOn"`
				} `json:"sgst"`
				Cgst struct {
					Percent   int    `json:"percent"`
					AppliedOn string `json:"appliedOn"`
				} `json:"cgst"`
			} `json:"tax"`
			ParentServiceID  string  `json:"parentServiceId"`
			PricePerMinute   float64 `json:"pricePerMinute"`
			ServiceType      string  `json:"serviceType"`
			CancelChargeTime int     `json:"cancelChargeTime"`
			Snooze           bool    `json:"snooze"`
		} `json:"serviceObj"`
		CouponCode     string `json:"couponCode"`
		PickupClusters string `json:"pickupClusters"`
		DropClusters   string `json:"dropClusters"`
		Distance       struct {
			FinalDistance float64 `json:"finalDistance"`
		} `json:"distance"`
		Polyline       string `json:"polyline"`
		CreatedDate    string `json:"createdDate"`
		V              int    `json:"__v"`
		OriginalQuotes []struct {
			Amount        int `json:"amount"`
			SubTotal      int `json:"subTotal"`
			Discount      int `json:"discount"`
			Surge         int `json:"surge"`
			Tax           int `json:"tax"`
			AmountBreakup struct {
				BaseFare struct {
					Total     int    `json:"total"`
					Key       string `json:"key"`
					Unit      int    `json:"unit"`
					Quantity  int    `json:"quantity"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"baseFare"`
				PrevDue struct {
					Total     int    `json:"total"`
					Key       string `json:"key"`
					Unit      int    `json:"unit"`
					Quantity  int    `json:"quantity"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"prevDue"`
				TollCharges struct {
					Total     int    `json:"total"`
					Key       string `json:"key"`
					Unit      int    `json:"unit"`
					Quantity  int    `json:"quantity"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"tollCharges"`
				PlatformCharges struct {
					Total     int    `json:"total"`
					Key       string `json:"key"`
					Unit      int    `json:"unit"`
					Quantity  int    `json:"quantity"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"platformCharges"`
				InsuranceCharges struct {
					Total     int    `json:"total"`
					Key       string `json:"key"`
					Unit      int    `json:"unit"`
					Quantity  int    `json:"quantity"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"insuranceCharges"`
				MinimumFare struct {
					Total     int    `json:"total"`
					Key       string `json:"key"`
					Unit      int    `json:"unit"`
					Quantity  int    `json:"quantity"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"minimumFare"`
				DistanceFare struct {
					Key       string  `json:"key"`
					Unit      int     `json:"unit"`
					Quantity  float64 `json:"quantity"`
					Total     float64 `json:"total"`
					Level     int     `json:"level"`
					Sign      string  `json:"sign"`
					ToShow    bool    `json:"toShow"`
					KeyToShow string  `json:"keyToShow"`
				} `json:"distanceFare"`
				TimeFare struct {
					Key       string  `json:"key"`
					Unit      float64 `json:"unit"`
					Quantity  float64 `json:"quantity"`
					Total     float64 `json:"total"`
					Level     int     `json:"level"`
					Sign      string  `json:"sign"`
					ToShow    bool    `json:"toShow"`
					KeyToShow string  `json:"keyToShow"`
				} `json:"timeFare"`
				Surge struct {
					Key       string `json:"key"`
					Total     int    `json:"total"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"surge"`
				Discount struct {
					Key       string `json:"key"`
					Total     int    `json:"total"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"discount"`
				Tax struct {
					Igst struct {
						Percent   int     `json:"percent"`
						AppliedOn string  `json:"appliedOn"`
						Key       string  `json:"key"`
						Total     float64 `json:"total"`
						Level     int     `json:"level"`
						Sign      string  `json:"sign"`
					} `json:"igst"`
					Sgst struct {
						Percent   int    `json:"percent"`
						AppliedOn string `json:"appliedOn"`
						Key       string `json:"key"`
						Total     int    `json:"total"`
						Level     int    `json:"level"`
						Sign      string `json:"sign"`
					} `json:"sgst"`
					Cgst struct {
						Percent   int     `json:"percent"`
						AppliedOn string  `json:"appliedOn"`
						Key       string  `json:"key"`
						Total     float64 `json:"total"`
						Level     int     `json:"level"`
						Sign      string  `json:"sign"`
					} `json:"cgst"`
					Total     int    `json:"total"`
					KeyToShow string `json:"keyToShow"`
					ToShow    bool   `json:"toShow"`
				} `json:"tax"`
				SubTotal struct {
					Key       string `json:"key"`
					Total     int    `json:"total"`
					Level     int    `json:"level"`
					Sign      string `json:"sign"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"subTotal"`
				Final struct {
					Key       string `json:"key"`
					Total     int    `json:"total"`
					Level     int    `json:"level"`
					ToShow    bool   `json:"toShow"`
					KeyToShow string `json:"keyToShow"`
				} `json:"final"`
			} `json:"amountBreakup"`
			Note             string `json:"note"`
			PriceDescription string `json:"priceDescription"`
			ServiceID        string `json:"serviceId"`
			Service          struct {
				ServiceID   string        `json:"serviceId"`
				MinimumFare int           `json:"minimumFare"`
				Extra       int           `json:"extra"`
				PricePerKm  int           `json:"pricePerKm"`
				Rule        string        `json:"rule"`
				BaseFare    int           `json:"baseFare"`
				PriceWithKm []interface{} `json:"priceWithKm"`
				PriceByKm   []struct {
					Price int `json:"price"`
					Kms   int `json:"kms"`
				} `json:"priceByKm"`
				City             string `json:"city"`
				CityID           string `json:"cityId"`
				CityRadius       int    `json:"cityRadius"`
				CancelCharge     int    `json:"cancelCharge"`
				Service          string `json:"service"`
				PlatformCharges  int    `json:"platformCharges"`
				InsuranceCharges int    `json:"insuranceCharges"`
				Tax              struct {
					Igst struct {
						Percent   int    `json:"percent"`
						AppliedOn string `json:"appliedOn"`
					} `json:"igst"`
					Sgst struct {
						Percent   int    `json:"percent"`
						AppliedOn string `json:"appliedOn"`
					} `json:"sgst"`
					Cgst struct {
						Percent   int    `json:"percent"`
						AppliedOn string `json:"appliedOn"`
					} `json:"cgst"`
				} `json:"tax"`
				ParentServiceID  string  `json:"parentServiceId"`
				PricePerMinute   float64 `json:"pricePerMinute"`
				ServiceType      string  `json:"serviceType"`
				CancelChargeTime int     `json:"cancelChargeTime"`
				Snooze           bool    `json:"snooze"`
			} `json:"service"`
			OfferApplied          bool   `json:"offerApplied,omitempty"`
			OfferType             string `json:"offerType,omitempty"`
			OfferID               string `json:"offerId,omitempty"`
			OfferCode             string `json:"offerCode,omitempty"`
			OfferText             string `json:"offerText,omitempty"`
			OfferFailureText      string `json:"offerFailureText,omitempty"`
			RideOfferPaymentType  string `json:"rideOfferPaymentType,omitempty"`
			OfferName             string `json:"offerName,omitempty"`
			PrevDue               int    `json:"prevDue"`
			RecomendedWalletPrice int    `json:"recomendedWalletPrice"`
			RecomendedWallet      string `json:"recomendedWallet"`
		} `json:"originalQuotes"`
		OldServices []interface{} `json:"oldServices"`
		Services    []struct {
			ServiceID   string        `json:"serviceId"`
			MinimumFare int           `json:"minimumFare"`
			Extra       int           `json:"extra"`
			PricePerKm  int           `json:"pricePerKm"`
			Rule        string        `json:"rule"`
			BaseFare    int           `json:"baseFare"`
			PriceWithKm []interface{} `json:"priceWithKm"`
			PriceByKm   []struct {
				Price int `json:"price"`
				Kms   int `json:"kms"`
			} `json:"priceByKm"`
			City             string `json:"city"`
			CityID           string `json:"cityId"`
			CityRadius       int    `json:"cityRadius"`
			CancelCharge     int    `json:"cancelCharge"`
			Service          string `json:"service"`
			PlatformCharges  int    `json:"platformCharges,omitempty"`
			InsuranceCharges int    `json:"insuranceCharges,omitempty"`
			Tax              struct {
				Igst struct {
					Percent   int    `json:"percent"`
					AppliedOn string `json:"appliedOn"`
				} `json:"igst"`
				Sgst struct {
					Percent   int    `json:"percent"`
					AppliedOn string `json:"appliedOn"`
				} `json:"sgst"`
				Cgst struct {
					Percent   int    `json:"percent"`
					AppliedOn string `json:"appliedOn"`
				} `json:"cgst"`
			} `json:"tax,omitempty"`
			ParentServiceID  string  `json:"parentServiceId"`
			PricePerMinute   float64 `json:"pricePerMinute"`
			ServiceType      string  `json:"serviceType"`
			CancelChargeTime int     `json:"cancelChargeTime"`
			Snooze           bool    `json:"snooze"`
		} `json:"services"`
		ServiceType     string `json:"serviceType"`
		CurrentLocation struct {
			Address string  `json:"address"`
			Lat     float64 `json:"lat"`
			Lng     float64 `json:"lng"`
		} `json:"currentLocation"`
		UserType   string   `json:"userType"`
		Type       string   `json:"type"`
		MapRiders  []string `json:"mapRiders"`
		BillAmount int      `json:"billAmount"`
		TimeBucket string   `json:"timeBucket"`
		WeekDay    int      `json:"weekDay"`
		PickHash   struct {
			Hash5 string `json:"hash5"`
			Hash6 string `json:"hash6"`
			Hash7 string `json:"hash7"`
		} `json:"pickHash"`
		DropHash struct {
			Hash5 string `json:"hash5"`
			Hash6 string `json:"hash6"`
			Hash7 string `json:"hash7"`
		} `json:"dropHash"`
		OrderDate      string `json:"orderDate"`
		CreatedOn      int64  `json:"createdOn"`
		LastModifiedOn int64  `json:"lastModifiedOn"`
		UniqueID       string `json:"uniqueId"`
		DeliveryOrder  bool   `json:"deliveryOrder"`
		Estimate       struct {
			Amount int `json:"amount"`
		} `json:"estimate"`
		CustomerObj struct {
			Name   string `json:"name"`
			Mobile string `json:"mobile"`
			Email  string `json:"email"`
			Gender string `json:"gender"`
			Device struct {
				Carrier      string `json:"carrier"`
				DeviceID     string `json:"deviceId"`
				Internet     string `json:"internet"`
				Manufacturer string `json:"manufacturer"`
				Model        string `json:"model"`
				AppID        string `json:"appId"`
			} `json:"device"`
			Dob           string `json:"dob"`
			FirebaseToken string `json:"firebaseToken"`
		} `json:"customerObj"`
		FraudRiders []interface{} `json:"fraudRiders"`
		FraudData   struct {
			MapRiders []string `json:"mapRiders"`
		} `json:"fraudData"`
		CouponObj struct {
			Usage bool `json:"usage"`
		} `json:"couponObj"`
		UpdatedAt []struct {
			ID        string `json:"_id"`
			Status    string `json:"status"`
			UpdatedAt int64  `json:"updatedAt"`
			Rider     string `json:"rider"`
		} `json:"updatedAt"`
		CancelReasons []string `json:"cancel_reasons"`
	}
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
