package main

import (
	"encoding/json"
	"errors"
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
type orderDetailEvents struct {
	ID string `json:"_id"`

	// PickupLocationLocationType []string `json:"pickupLocation_locationType"`

	PickupLocationAddress string  `json:"pickupLocation_address"`
	PickupLocationLat     float64 `json:"pickupLocation_lat"`
	PickupLocationLng     float64 `json:"pickupLocation_lng"`
	PickupLocationCurrLat float64 `json:"pickupLocation_currLat"`
	PickupLocationCurrLng float64 `json:"pickupLocation_currLng"`
	PickupLocationSetLat  float64 `json:"pickupLocation_setLat"`
	PickupLocationSetLng  float64 `json:"pickupLocation_setLng"`

	// DropLocationLocationType []string `json:"dropLocation_locationType"`

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

	ServiceObjPriceByKmPrice int `json:"serviceObj_priceByKm_price"`
	ServiceObjPriceByKmKms   int `json:"serviceObj_priceByKm_kms"`

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

	// FraudRiders []interface{} `json:"fraudRiders"`

	//fod.FraudDataMapRiders = strings.Join(od.FraudData.MapRiders, ",")
	FraudDataMapRiders []string `json:"fraudData_mapRiders"`

	CouponObjUsage bool `json:"couponObj_usage"`

	CancelReasons []string `json:"cancel_reasons"`
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
		PrevDueIds        string        `json:"prevDueIds"`
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

	// if len(od.PickupClustersAll) <= 0 {
	// 	fod.PickupClustersAll = ""
	// } else if len(od.PickupClustersAll) == 1 {
	// 	fod.PickupClustersAll = od.PickupClustersAll[0]
	// } else {
	// 	fod.PickupClustersAll = strings.Join(od.PickupClustersAll, ",")
	// }

	return []byte(""), nil
}
func (o *orderDetailEvents) Unmarshal(data []byte) error {
	return json.Unmarshal(data, o)
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
	return json.Marshal(orderData)
}
func (o *orderEvents) Unmarshal(data []byte) error {
	return json.Unmarshal(data, o)
}
func (e *etaEvents) Marshal() ([]byte, error) {
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
func (e *etaEvents) Unmarshal(data []byte) error {
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
