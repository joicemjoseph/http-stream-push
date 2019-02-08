package kafkareader

import (
	"context"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	kafka "github.com/segmentio/kafka-go"
	_ "github.com/segmentio/kafka-go/gzip" // ETA is gzipped.
)

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
}

// Read data from kafka
func (c *Config) Read(ctx context.Context, offset *int64, bufferSize, partitionSize *int) (chan KafkaResult, *sync.WaitGroup, error) {
	msgChan := make(chan KafkaResult, (*bufferSize)*(*partitionSize))
	wg2 := new(sync.WaitGroup)
	var err error

	for partition := 0; partition < *partitionSize; partition++ {
		wg2.Add(1)
		reader(ctx, wg2, c, offset, bufferSize, partition, msgChan)
	}
	return msgChan, wg2, err
}
func reader(ctx context.Context, wg2 *sync.WaitGroup, c *Config, offset *int64, bufferSize *int, partition int, msgChan chan KafkaResult) {

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{*c.kafkaURL},
		Topic:     *c.topicName,
		Partition: partition,
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
		defer wg2.Done()
		var counter int64
		for {
			select {
			default:

				m, err := r.ReadMessage(ctx)

				if err != nil {
					log.Error().
						Int64("counter", m.Offset).
						Int("partition", m.Partition).
						Str("error", err.Error()).
						Msg("Error occured")

					continue
				}

				log.Info().
					Int64("counter", counter+1).
					Int("partition", m.Partition).
					Msgf("%+v", string(m.Value))
				kr := KafkaResult{
					Message:   m.Value,
					Err:       err,
					Counter:   m.Offset,
					Partition: m.Partition,
				}
				msgChan <- kr
				counter = m.Offset
			case <-ctx.Done():

				log.Info().Int64("counter", counter).
					Int("partition", partition).
					Msg("Ending gracefully. Last counter")

				return
			}
		}
	}()
	// data, _ := json.Marshal(m)
	// return msgChan, nil
}
