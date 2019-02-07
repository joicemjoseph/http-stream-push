package main

import (
	"encoding/json"
	"os"
	"os/signal"
	"sync"
	"syscall"

	reader "github.com/joicemjoseph/http-stream-push/kafkareader"
	writer "github.com/joicemjoseph/http-stream-push/kafkawriter"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

}
func main() {
	kafkaReaderURL, kafkaReaderTopic, kafkaReaderOffset,
		kafkaWriterTopic, kafkaWriterURL, bufferSize := parse()
	stopReading := make(chan os.Signal, 1)
	cfg := broaker{reader: reader.Create(kafkaReaderTopic, kafkaReaderURL),
		writer: writer.Create(kafkaWriterTopic, kafkaWriterURL)}
	structData, err := getStruct(*kafkaReaderTopic)
	if err != nil {
		log.Warn().Msgf(*kafkaReaderTopic + " is " + err.Error())
		panic(err)
	}
	signal.Notify(stopReading, syscall.SIGINT, syscall.SIGTERM) //syscall.SIGABRT, syscall.SIGINT
	mp, err := cfg.reader.Read(kafkaReaderOffset, bufferSize, stopReading)

	// if ok {
	// 	log.Info().Msgf("%+v", m.Err)
	// 	fmt.Print(reader.KafkaResult(m).message)
	// }

	if err != nil {
		log.Warn().Msgf(err.Error())
	}

	xthreads := 400
	var wg sync.WaitGroup
	wg.Add(xthreads)
	for i := 0; i < xthreads; i++ {
		go func() {

			for {
				data, ok := <-mp
				if !ok { // if there is nothing to do and the channel has been closed then end the goroutine
					wg.Done()
					return
				}
				if data.Err != nil {
					log.Error().Msg(err.Error())
					continue
				}
				job(data, structData, cfg)
			}
		}()
	}
	wg.Wait()

}
func job(mp reader.KafkaResult, data Data, cfg broaker) {
	err := json.Unmarshal(mp.Message, data)
	if err != nil {
		log.Error().Int64("Counter", mp.Counter).Msgf(err.Error())
		return
	}
	converted, err := json.Marshal(data)
	if err != nil {
		log.Error().Int64("Counter", mp.Counter).Msgf(err.Error())
		return
	}
	log.Info().Str("message", string(converted)).Msg("")
	err = cfg.writer.Push(&converted)
	if err != nil {
		log.Error().Int64("Counter", mp.Counter).Msgf(err.Error())
		return
	}
}
