package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	reader "github.com/joicemjoseph/http-stream-push/kafkareader"
)

const (
	kafkaDefaultReaderURL    = "http://localhost"
	kafkaDefaultReaderTopic  = "test"
	kafkaDefaultReaderOffset = 0
	kafkaDefaultBufferSize   = 1

	kafkaDefaultWriterURL   = "http://localhost"
	kafkaDefaultWriterTopic = "test2"
)
const (
	kafkaReaderURLENV    = "KAFKA_READER_URL"
	kafkaReaderTopicENV  = "KAFKA_READER_TOPIC"
	kafkaReaderOffsetENV = "KAFKA_READER_OFFSET"

	kafkaBufferSizeENV    = "KAFKA_BUFFER_SIZE"
	kafkaPartitionSizeENV = "KAFKA_PARTITION_SIZE"

	kafkaWriterURLENV   = "KAFKA_WRITER_URL"
	kafkaWriterTopicENV = "KAFKA_WRITER_TOPIC"
)

var client *http.Client

// Order is orderEvents flattened to send data to kafka.
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

// orderEvents read data from topic, order_events
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
type broaker struct {
	reader ReadData
	writer WriteData
}

// etaEvents to read from topic eta
type etaEvents struct {
	DateTime     int64  `json:"dateTime"`
	UserID       string `json:"userId"`
	PickLocation struct {
		Lat float64 `json:"lat"`
		Lng float64 `json:"lng"`
	} `json:"pickLocation"`
	DropLocation struct {
		Lat float64 `json:"lat"`
		Lng float64 `json:"lng"`
	} `json:"dropLocation,omitempty"`
	PickCluster string `json:"pickCluster"`
	DropCluster string `json:"dropCluster"`
	Snooze      bool   `json:"snooze"`
}

// eta is for flattening etaEvents.
type eta struct {
	DateTime int64  `json:"dateTime"`
	UserID   string `json:"userId"`

	PickLocationLat float64 `json:"pickLocation_lat"`
	PickLocationLng float64 `json:"pickLocation_lng"`

	DropLocationLat float64 `json:"dropLocation_lat,omitempty"`
	DropLocationLng float64 `json:"dropLocation_lng,omitempty"`

	PickCluster string `json:"pickCluster"`
	DropCluster string `json:"dropCluster"`
	Snooze      bool   `json:"snooze"`
}

// order_detail_events flattened.
type orderDetails struct {
	ID string `json:"_id"`

	PickupLocationLocationType string `json:"pickupLocation_locationType"`

	PickupLocationAddress string  `json:"pickupLocation_address"`
	PickupLocationLat     float64 `json:"pickupLocation_lat"`
	PickupLocationLng     float64 `json:"pickupLocation_lng"`
	PickupLocationCurrLat float64 `json:"pickupLocation_currLat"`
	PickupLocationCurrLng float64 `json:"pickupLocation_currLng"`
	PickupLocationSetLat  float64 `json:"pickupLocation_setLat"`
	PickupLocationSetLng  float64 `json:"pickupLocation_setLng"`

	DropLocationLocationType string `json:"dropLocation_locationType"`

	DropLocationAddress string  `json:"dropLocation_address"`
	DropLocationLat     float64 `json:"dropLocation_lat"`
	DropLocationLng     float64 `json:"dropLocation_lng"`
	DropLocationSetLat  float64 `json:"dropLocation_setLat"`
	DropLocationSetLng  float64 `json:"dropLocation_setLng"`

	CollectedCash       int `json:"collected_cash"`
	CollectedPaytm      int `json:"collected_paytm"`
	CollectedRapido     int `json:"collected_rapido"`
	CollectedWallet     int `json:"collected_wallet"`
	CollectedCwallet    int `json:"collected_cwallet"`
	CollectedMobikwik   int `json:"collected_mobikwik"`
	CollectedFreecharge int `json:"collected_freecharge"`
	CollectedLazypay    int `json:"collected_lazypay"`

	FeedbackCustomerRated bool `json:"feedback_customerRated"`
	// Join strings based of array.
	// strings.Join(customerRateService, ",")
	FeedbackCustomerRateService string `json:"feedback_customerRateService"`

	FeedbackReviewStatus bool `json:"feedback_reviewStatus"`

	RequestID        string  `json:"requestId"`
	UserFirstBooking bool    `json:"userFirstBooking"`
	FakeGps          bool    `json:"fakeGps"`
	PaymentStatus    string  `json:"paymentStatus"`
	OrderType        string  `json:"orderType"`
	ClientID         string  `json:"clientId"`
	RideTime         float64 `json:"rideTime"`
	Discount         int     `json:"discount"`
	SubTotal         int     `json:"subTotal"`
	Amount           int     `json:"amount"`
	CancelFee        int     `json:"cancelFee"`
	PrevDue          int     `json:"prevDue"`
	// strings.Join(PrevDueIds, ",")
	PrevDueIds string `json:"prevDueIds"`
	TaxPercent int    `json:"taxPercent"`
	TaxAmount  int    `json:"taxAmount"`
	CashBack   int    `json:"cashBack"`
	//if val, ok := od.UserCashbackType.(string); ok {
	//	fod.UserCashbackType = val
	//} else {
	//	fod.UserCashbackType = "null"
	//}

	UserCashbackType string `json:"userCashbackType"`
	HailingVerified  bool   `json:"hailingVerified"`
	Status           string `json:"status"`
	//if len(od.PickupClustersAll) <= 0 {
	//	fod.PickupClustersAll = ""
	//} else if len(od.PickupClustersAll) == 1 {
	//	fod.PickupClustersAll = od.PickupClustersAll[0]
	//} else {
	//	fod.PickupClustersAll = strings.Join(od.PickupClustersAll, ",")
	//}
	PickupClustersAll string `json:"pickupClustersAll"`

	//if len(od.DropClustersAll) <= 0 {
	//	fod.DropClustersAll = ""
	//} else if len(od.DropClustersAll) == 1 {
	//	fod.DropClustersAll = od.DropClustersAll[0]
	//} else {
	//	fod.DropClustersAll = strings.Join(od.DropClustersAll, ",")
	//}
	DropClustersAll string `json:"dropClustersAll"`
	//fod.PrevRiders = strings.Join(od.PrevRiders, ",")
	PrevRiders  string `json:"prevRiders"`
	PaymentType string `json:"paymentType"`

	CustomerID        string `json:"customer__id"`
	CustomerMobile    string `json:"customer_mobile"`
	CustomerEmail     string `json:"customer_email"`
	CustomerFirstName string `json:"customer_firstName"`
	CustomerGender    int    `json:"customer_gender"`
	CustomerLastName  string `json:"customer_lastName"`

	ServiceObjServiceID   string `json:"serviceObj_serviceId"`
	ServiceObjMinimumFare int    `json:"serviceObj_minimumFare"`
	ServiceObjExtra       int    `json:"serviceObj_extra"`
	ServiceObjPricePerKm  int    `json:"serviceObj_pricePerKm"`
	ServiceObjRule        string `json:"serviceObj_rule"`
	ServiceObjBaseFare    int    `json:"serviceObj_baseFare"`

	//ServiceObjPriceWithKm []interface{} `json:"serviceObj_priceWithKm"`

	// ServiceObjPriceByKmPrice int `json:"serviceObj_priceByKm_price"`
	// ServiceObjPriceByKmKms   int `json:"serviceObj_priceByKm_kms"`

	ServiceObjCity             string `json:"serviceObj_city"`
	ServiceObjCityID           string `json:"serviceObj_cityId"`
	ServiceObjCityRadius       int    `json:"serviceObj_cityRadius"`
	ServiceObjCancelCharge     int    `json:"serviceObj_cancelCharge"`
	ServiceObjService          string `json:"serviceObj_service"`
	ServiceObjPlatformCharges  int    `json:"serviceObj_platformCharges"`
	ServiceObjInsuranceCharges int    `json:"serviceObj_insuranceCharges"`

	ServiceObjTaxIgstPercent   int    `json:"serviceObj_tax_igst_percent"`
	ServiceObjTaxIgstAppliedOn string `json:"serviceObj_tax_igst_appliedOn"`

	ServiceObjTaxSgstPercent   int    `json:"serviceObj_tax_sgst_percent"`
	ServiceObjTaxSgstAppliedOn string `json:"serviceObj_tax_sgst_appliedOn"`

	ServiceObjTaxCgstPercent   int    `json:"serviceObj_tax_cgst_percent"`
	ServiceObjTaxCgstAppliedOn string `json:"serviceObj_tax_cgst_appliedOn"`

	ServiceObjParentServiceID  string  `json:"serviceObj_parentServiceId"`
	ServiceObjPricePerMinute   float64 `json:"serviceObj_pricePerMinute"`
	ServiceObjServiceType      string  `json:"serviceObj_serviceType"`
	ServiceObjCancelChargeTime int     `json:"serviceObj_cancelChargeTime"`
	ServiceObjSnooze           bool    `json:"serviceObj_snooze"`

	CouponCode     string `json:"couponCode"`
	PickupClusters string `json:"pickupClusters"`
	DropClusters   string `json:"dropClusters"`

	DistanceFinalDistance float64 `json:"distance_finalDistance"`

	Polyline    string `json:"polyline"`
	CreatedDate string `json:"createdDate"`
	V           int    `json:"__v"`

	// OldServices []interface{} `json:"oldServices"`

	ServiceType string `json:"serviceType"`

	CurrentLocationAddress string  `json:"currentLocation_address"`
	CurrentLocationLat     float64 `json:"currentLocation_lat"`
	CurrentLocationLng     float64 `json:"currentLocation_lng"`

	UserType string `json:"userType"`
	Type     string `json:"type"`
	// fod.MapRiders = strings.Join(od.MapRiders, ",")
	MapRiders  string `json:"mapRiders"`
	BillAmount int    `json:"billAmount"`
	TimeBucket string `json:"timeBucket"`
	WeekDay    int    `json:"weekDay"`

	PickHashHash5 string `json:"pickHash_hash5"`
	PickHashHash6 string `json:"pickHash_hash6"`
	PickHashHash7 string `json:"pickHash_hash7"`

	DropHashHash5 string `json:"dropHash_hash5"`
	DropHashHash6 string `json:"dropHash_hash6"`
	DropHashHash7 string `json:"dropHash_hash7"`

	OrderDate      string `json:"orderDate"`
	CreatedOn      int64  `json:"createdOn"`
	LastModifiedOn int64  `json:"lastModifiedOn"`
	UniqueID       string `json:"uniqueId"`
	DeliveryOrder  bool   `json:"deliveryOrder"`

	EstimateAmount int `json:"estimate_amount"`

	CustomerObjName   string `json:"customerObj_name"`
	CustomerObjMobile string `json:"customerObj_mobile"`
	CustomerObjEmail  string `json:"customerObj_email"`
	CustomerObjGender string `json:"customerObj_gender"`

	CustomerObjDeviceCarrier      string `json:"customerObj_device_carrier"`
	CustomerObjDeviceDeviceID     string `json:"customerObj_device_deviceId"`
	CustomerObjDeviceInternet     string `json:"customerObj_device_internet"`
	CustomerObjDeviceManufacturer string `json:"customerObj_device_manufacturer"`
	CustomerObjDeviceModel        string `json:"customerObj_device_model"`
	CustomerObjDeviceAppID        string `json:"customerObj_device_appId"`

	CustomerObjDob           string `json:"customerObj_dob"`
	CustomerObjFirebaseToken string `json:"customerObj_firebaseToken"`

	// FraudRiders string `json:"fraudRiders"`

	//fod.FraudDataMapRiders = strings.Join(od.FraudData.MapRiders, ",")
	FraudDataMapRiders string `json:"fraudData_mapRiders"`

	CouponObjUsage bool `json:"couponObj_usage"`

	CancelReasons string `json:"cancel_reasons"`
}

// orderDetailEvents to read from kafka topic order_detail_events
type orderDetailEvents struct {
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
		CustomerRated       bool     `json:"customerRated"`
		CustomerRateService []string `json:"customerRateService"`
		ReviewStatus        bool     `json:"reviewStatus"`
	} `json:"feedback"`
	RequestID        string  `json:"requestId"`
	UserFirstBooking bool    `json:"userFirstBooking"`
	FakeGps          bool    `json:"fakeGps"`
	PaymentStatus    string  `json:"paymentStatus"`
	OrderType        string  `json:"orderType"`
	ClientID         string  `json:"clientId"`
	RideTime         float64 `json:"rideTime"`
	Discount         int     `json:"discount"`
	SubTotal         int     `json:"subTotal"`
	Amount           int     `json:"amount"`
	CancelFee        int     `json:"cancelFee"`
	PrevDue          int     `json:"prevDue"`
	//strings.Join(od.PrevRiders, ",")
	PrevDueIds        string      `json:"prevDueIds"`
	TaxPercent        int         `json:"taxPercent"`
	TaxAmount         int         `json:"taxAmount"`
	CashBack          int         `json:"cashBack"`
	UserCashbackType  interface{} `json:"userCashbackType"`
	HailingVerified   bool        `json:"hailingVerified"`
	Status            string      `json:"status"`
	PickupClustersAll []string    `json:"pickupClustersAll"`
	DropClustersAll   []string    `json:"dropClustersAll"`
	PrevRiders        []string    `json:"prevRiders"`
	PaymentType       string      `json:"paymentType"`
	Customer          struct {
		ID        string `json:"_id"`
		Mobile    string `json:"mobile"`
		Email     string `json:"email"`
		FirstName string `json:"firstName"`
		Gender    int    `json:"gender"`
		LastName  string `json:"lastName"`
	} `json:"customer"`
	ServiceObj struct {
		ServiceID   string `json:"serviceId"`
		MinimumFare int    `json:"minimumFare"`
		Extra       int    `json:"extra"`
		PricePerKm  int    `json:"pricePerKm"`
		Rule        string `json:"rule"`
		BaseFare    int    `json:"baseFare"`
		// PriceWithKm []interface{} `json:"priceWithKm"`
		// PriceByKm   []struct {
		// 	Price int `json:"price"`
		// 	Kms   int `json:"kms"`
		// } `json:"priceByKm"`
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
	Polyline    string `json:"polyline"`
	CreatedDate string `json:"createdDate"`
	V           int    `json:"__v"`

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
	FraudRiders []string `json:"fraudRiders"`
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

// ReadData is to read data.
// It can be either file, db or kafka
type ReadData interface {

	// read stream data
	Read(context.Context, *int64, *int, *int) (chan reader.KafkaResult, *sync.WaitGroup, error)
}

// WriteData to write data.
// It can be file, DB or kafka
type WriteData interface {
	// push data to
	Push(*[]byte, int) error
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
	MarshalJSON() ([]byte, error)
}

// For Testing only
func (o *order) MarshalJSON() ([]byte, error) {
	return []byte(""), nil
}

func (o *orderDetailEvents) MarshalJSON() ([]byte, error) {

	pickupClustersAll := ""
	if len(o.PickupClustersAll) <= 0 {
		pickupClustersAll = ""
	} else if len(o.PickupClustersAll) == 1 {
		pickupClustersAll = o.PickupClustersAll[0]
	} else {
		pickupClustersAll = strings.Join(o.PickupClustersAll, ",")
	}
	userCashBack, ok := o.UserCashbackType.(string)
	if !ok {
		userCashBack = "null"
	}
	dropClustersAll := ""
	if len(o.DropClustersAll) <= 0 {
		dropClustersAll = ""
	} else if len(o.DropClustersAll) == 1 {
		dropClustersAll = o.DropClustersAll[0]
	} else {
		dropClustersAll = strings.Join(o.DropClustersAll, ",")
	}
	events := orderDetails{
		Amount:                        o.Amount,
		BillAmount:                    o.BillAmount,
		CancelFee:                     o.CancelFee,
		CancelReasons:                 strings.Join(o.CancelReasons, ", "),
		CashBack:                      o.CashBack,
		ClientID:                      o.ClientID,
		CollectedCash:                 o.Collected.Cash,
		CollectedCwallet:              o.Collected.Cwallet,
		CollectedFreecharge:           o.Collected.Freecharge,
		CollectedLazypay:              o.Collected.Lazypay,
		CollectedMobikwik:             o.Collected.Mobikwik,
		CollectedPaytm:                o.Collected.Paytm,
		CollectedRapido:               o.Collected.Rapido,
		CollectedWallet:               o.Collected.Wallet,
		CouponCode:                    o.CouponCode,
		CouponObjUsage:                o.CouponObj.Usage,
		CreatedDate:                   o.CreatedDate,
		CreatedOn:                     o.CreatedOn,
		CurrentLocationAddress:        o.CurrentLocation.Address,
		CurrentLocationLat:            o.CurrentLocation.Lat,
		CurrentLocationLng:            o.CurrentLocation.Lng,
		CustomerEmail:                 o.Customer.Email,
		CustomerFirstName:             o.Customer.FirstName,
		CustomerGender:                o.Customer.Gender,
		CustomerID:                    o.Customer.ID,
		CustomerLastName:              o.Customer.LastName,
		CustomerMobile:                o.Customer.Mobile,
		CustomerObjDeviceAppID:        o.CustomerObj.Device.AppID,
		CustomerObjDeviceCarrier:      o.CustomerObj.Device.Carrier,
		CustomerObjDeviceDeviceID:     o.CustomerObj.Device.DeviceID,
		CustomerObjDeviceInternet:     o.CustomerObj.Device.Internet,
		CustomerObjDeviceManufacturer: o.CustomerObj.Device.Manufacturer,
		CustomerObjDeviceModel:        o.CustomerObj.Device.Model,
		CustomerObjGender:             o.CustomerObj.Gender,
		CustomerObjDob:                o.CustomerObj.Dob,
		CustomerObjEmail:              o.CustomerObj.Email,
		CustomerObjFirebaseToken:      o.CustomerObj.FirebaseToken,
		CustomerObjMobile:             o.CustomerObj.Mobile,
		CustomerObjName:               o.CustomerObj.Name,
		DeliveryOrder:                 o.DeliveryOrder,
		Discount:                      o.Discount,
		DistanceFinalDistance:         o.Distance.FinalDistance,
		DropClusters:                  o.DropClusters,
		DropClustersAll:               dropClustersAll,
		DropHashHash5:                 o.DropHash.Hash5,
		DropHashHash6:                 o.DropHash.Hash6,
		DropHashHash7:                 o.DropHash.Hash7,
		DropLocationAddress:           o.DropLocation.Address,
		DropLocationLat:               o.DropLocation.Lat,
		DropLocationLng:               o.DropLocation.Lng,
		DropLocationSetLat:            o.DropLocation.SetLat,
		DropLocationSetLng:            o.DropLocation.SetLng,
		DropLocationLocationType:      strings.Join(o.DropLocation.LocationType, ", "),
		EstimateAmount:                o.Estimate.Amount,
		FakeGps:                       o.FakeGps,
		FeedbackCustomerRateService:   strings.Join(o.Feedback.CustomerRateService, ", "),
		FeedbackCustomerRated:         o.Feedback.CustomerRated,
		FeedbackReviewStatus:          o.Feedback.ReviewStatus,
		FraudDataMapRiders:            strings.Join(o.FraudData.MapRiders, ", "),
		HailingVerified:               o.HailingVerified,
		ID:                            o.ID,
		LastModifiedOn:                o.LastModifiedOn,
		MapRiders:                     strings.Join(o.MapRiders, ", "),
		OrderDate:                     o.OrderDate,
		OrderType:                     o.OrderType,
		PaymentStatus:                 o.PaymentStatus,
		PaymentType:                   o.PaymentType,
		PickHashHash5:                 o.PickHash.Hash5,
		PickHashHash6:                 o.PickHash.Hash6,
		PickHashHash7:                 o.PickHash.Hash7,
		PickupClusters:                o.PickupClusters,
		PickupClustersAll:             pickupClustersAll,
		PickupLocationAddress:         o.PickupLocation.Address,
		PickupLocationCurrLat:         o.PickupLocation.CurrLat,
		PickupLocationCurrLng:         o.PickupLocation.CurrLng,
		PickupLocationLat:             o.PickupLocation.Lat,
		PickupLocationLng:             o.PickupLocation.Lng,
		PickupLocationLocationType:    strings.Join(o.PickupLocation.LocationType, ", "),
		PickupLocationSetLat:          o.PickupLocation.SetLat,
		PickupLocationSetLng:          o.PickupLocation.SetLng,
		Polyline:                      o.Polyline,
		PrevDue:                       o.PrevDue,
		PrevDueIds:                    o.PrevDueIds,
		PrevRiders:                    strings.Join(o.PrevRiders, ", "),
		RequestID:                     o.RequestID,
		RideTime:                      o.RideTime,
		ServiceObjBaseFare:            o.ServiceObj.BaseFare,
		ServiceObjCancelCharge:        o.ServiceObj.CancelCharge,
		ServiceObjCity:                o.ServiceObj.City,
		ServiceObjCityID:              o.ServiceObj.CityID,
		ServiceObjCityRadius:          o.ServiceObj.CityRadius,
		ServiceObjExtra:               o.ServiceObj.Extra,
		ServiceObjInsuranceCharges:    o.ServiceObj.InsuranceCharges,
		ServiceObjMinimumFare:         o.ServiceObj.MinimumFare,
		ServiceObjParentServiceID:     o.ServiceObj.ParentServiceID,
		ServiceObjPlatformCharges:     o.ServiceObj.PlatformCharges,
		// ServiceObjPriceByKmKms: o.ServiceObj.PriceByKm ,
		ServiceObjPricePerKm:       o.ServiceObj.PricePerKm,
		ServiceObjPricePerMinute:   o.ServiceObj.PricePerMinute,
		ServiceObjRule:             o.ServiceObj.Rule,
		ServiceObjService:          o.ServiceObj.Service,
		ServiceObjServiceID:        o.ServiceObj.ServiceID,
		ServiceObjServiceType:      o.ServiceObj.ServiceType,
		ServiceObjSnooze:           o.ServiceObj.Snooze,
		ServiceObjTaxCgstAppliedOn: o.ServiceObj.Tax.Cgst.AppliedOn,
		ServiceObjTaxCgstPercent:   o.ServiceObj.Tax.Cgst.Percent,
		ServiceObjTaxIgstAppliedOn: o.ServiceObj.Tax.Igst.AppliedOn,
		ServiceObjTaxIgstPercent:   o.ServiceObj.Tax.Igst.Percent,
		ServiceObjTaxSgstAppliedOn: o.ServiceObj.Tax.Sgst.AppliedOn,
		ServiceObjTaxSgstPercent:   o.ServiceObj.Tax.Sgst.Percent,
		ServiceType:                o.ServiceType,
		Status:                     o.Status,
		SubTotal:                   o.SubTotal,
		TaxAmount:                  o.TaxAmount,
		TaxPercent:                 o.TaxPercent,
		TimeBucket:                 o.TimeBucket,
		Type:                       o.Type,
		UniqueID:                   o.UniqueID,
		UserCashbackType:           userCashBack,
		UserFirstBooking:           o.UserFirstBooking,
		UserType:                   o.UserType,
		V:                          o.V,
		WeekDay:                    o.WeekDay,
	}
	return json.Marshal(events)
}

func (o *orderEvents) MarshalJSON() ([]byte, error) {

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
	return json.Marshal(orderData)
}

func (e *etaEvents) MarshalJSON() ([]byte, error) {
	etaData := eta{
		DateTime:        e.DateTime,
		DropCluster:     e.DropCluster,
		DropLocationLat: e.DropLocation.Lat,
		DropLocationLng: e.DropLocation.Lng,
		PickCluster:     e.PickCluster,
		PickLocationLat: e.PickLocation.Lat,
		PickLocationLng: e.PickLocation.Lng,
		Snooze:          e.Snooze,
		UserID:          e.UserID,
	}
	return json.Marshal(etaData)
}

func getStruct(topic string) (Data, error) {
	name := []string{"order_events", "eta", "order_detail_events", "sample"}
	if !contains(&name, &topic) {
		return nil, errors.New("Not a valid topic")
	}
	if name[0] == topic {
		return &orderEvents{}, nil
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
func parse() (*string, *string, *int64, *string, *string, *int, *int) {
	// parse flags

	offset, _ := strconv.ParseInt(os.Getenv(kafkaReaderOffsetENV), 10, 64)
	bufferSize, _ := strconv.Atoi(os.Getenv(kafkaBufferSizeENV))
	partitionSize, _ := strconv.Atoi(os.Getenv(kafkaPartitionSizeENV))
	kafkaReaderURL := flag.String("in-url", os.Getenv(kafkaReaderURLENV), "URL of the kafka server to read data from")
	kafkaReaderTopic := flag.String("in-topic", os.Getenv(kafkaReaderTopicENV), "name of the topic to read the stream")
	kafkaOffset := flag.Int64("offset", offset, "offset number")
	KafkaWriterURL := flag.String("out-url", os.Getenv(kafkaWriterURLENV), "URL of kafka server to write data to")
	kafkaWriterTopic := flag.String("out-topic", os.Getenv(kafkaWriterTopicENV), "Name of topic to write the stream")
	kafkaBufferSize := flag.Int("buffer-size", bufferSize, "Buffer size for reading")
	kafkaPartitionSize := flag.Int("partition-size", partitionSize, "number of partitions in-topic have")
	flag.Parse()

	if *kafkaReaderURL == "" {
		*kafkaReaderURL = kafkaDefaultReaderURL
	}
	if *kafkaReaderTopic == "" {
		*kafkaReaderTopic = kafkaDefaultReaderTopic
	}
	if *kafkaOffset < 0 {
		*kafkaOffset = kafkaDefaultReaderOffset
	}
	if *kafkaBufferSize <= 0 {
		*kafkaBufferSize = kafkaDefaultBufferSize
	}
	if flag.Parsed() && (*kafkaReaderURL == "" || *kafkaReaderTopic == "" || *kafkaOffset < 0 || *KafkaWriterURL == "" || *kafkaWriterTopic == "" || *kafkaBufferSize < 0) {
		flag.PrintDefaults()
		os.Exit(1)
	}
	if *KafkaWriterURL == "" {
		*KafkaWriterURL = kafkaDefaultWriterURL
	}
	if *kafkaWriterTopic == "" {
		*kafkaWriterTopic = kafkaDefaultWriterTopic
	}
	// if write.Parsed() && (*KafkaWriterURL == "" || *kafkaWriterTopic == "") {
	// 	write.PrintDefaults()
	// 	os.Exit(1)
	// }
	return kafkaReaderURL, kafkaReaderTopic, kafkaOffset, kafkaWriterTopic, KafkaWriterURL, kafkaBufferSize, kafkaPartitionSize
}
