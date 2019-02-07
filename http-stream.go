package main

import (
	"encoding/json"

	reader "github.com/joicemjoseph/http-stream-push/kafkareader"
	writer "github.com/joicemjoseph/http-stream-push/kafkawriter"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func init() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	log.Error().Str("key", "value").Msgf("info logging enabled")

}
func main() {
	kafkaReaderURL, kafkaReaderTopic, kafkaReaderOffset, kafkaWriterTopic, kafkaWriterURL := parse()

	cfg := broaker{reader: reader.Create(kafkaReaderTopic, kafkaReaderURL),
		writer: writer.Create(kafkaWriterTopic, kafkaWriterURL)}
	structData, err := getStruct(*kafkaReaderTopic)
	if err != nil {
		log.Warn().Msgf(*kafkaReaderTopic + " is " + err.Error())
		panic(err)
	}
	mp, err := cfg.reader.Read(kafkaReaderOffset)
	if err != nil {
		log.Warn().Msgf(err.Error())
	}
	err = json.Unmarshal(*mp, structData)
	if err != nil {
		log.Warn().Msgf(err.Error())

	}
	converted, err := json.Marshal(structData)
	if err != nil {
		log.Warn().Msgf(err.Error())

	}
	log.Info().Str("message", string(converted)).Msg("")
	err = cfg.writer.Push(&converted)
	if err != nil {
		log.Fatal().Msgf(err.Error())
	}
	log.Fatal().Msgf(err.Error())
}
