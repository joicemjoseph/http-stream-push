package kafkareader

import (
	"context"
	"os"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip" // ETA is gzipped.
)

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

// Read data from kafka
func (c *Config) Read(offset *int64, bufferSize *int, stopReading chan os.Signal) (chan KafkaResult, error) {
	return reader(c, offset, bufferSize, stopReading)
}
func reader(c *Config, offset *int64, bufferSize *int, stopReading chan os.Signal) (chan KafkaResult, error) {
	msgChan := make(chan KafkaResult, *bufferSize)

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{*c.kafkaURL},
		Topic:     *c.topicName,
		Partition: 0,
		MinBytes:  1,   // 10KB
		MaxBytes:  1e5, // 1MB

	})

	r.SetOffset(*offset)
	// m, err := r.ReadMessage(context.Background())
	// if err != nil {
	// 	panic(err)
	// }
	// log.Printf("%+v", string(m.Value))

	go func() {
		defer r.Close()
		var counter = int64(0) + *offset
		for {
			select {
			default:
				m, err := r.ReadMessage(context.Background())
				if err != nil {
					log.Error().
						Int64("counter", counter).
						Str("error", err.Error()).
						Msg("Error occured")
					counter++
					continue
				}
				log.Printf("this is :%+v", string(m.Value))
				kr := KafkaResult{
					Message: m.Value,
					Err:     err,
					Counter: counter,
				}
				msgChan <- kr
				counter++
			case <-stopReading:

				log.Info().Int64("counter", counter).
					Msg("Ending gracefully. Last counter")
				close(msgChan)
				return
			}
		}
	}()
	// data, _ := json.Marshal(m)
	return msgChan, nil
}
