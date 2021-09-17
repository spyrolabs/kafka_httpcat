package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	broker := os.Getenv("KAFKA_BROKER")
	group := os.Getenv("KAFKA_CONSUMER_GROUP")
	topic := os.Getenv("KAFKA_TOPIC")

	openTsdbBosunUrl := os.Getenv("OPEN_TSDB_BOSUN_URL")
	openTsdbShortTermRetentionUrl := os.Getenv("OPEN_TSDB_SHORT_TERM_URL")
	openTsdbLongTermRetentionUrl := os.Getenv("OPEN_TSDB_LONG_TERM_URL")
	openTsdbLegacyUrl := os.Getenv("OPEN_TSDB_LEGACY_URL")

	openTsdbBosunDiscardRatio, _ := strconv.Atoi(os.Getenv("OPEN_TSDB_BOSUN_DISCARD_RATIO"))
	openTsdbShortTermRetentionDiscardRatio, _ := strconv.Atoi(os.Getenv("OPEN_TSDB_SHORT_TERM_DISCARD_RATIO"))
	openTsdbLongTermRetentionDiscardRatio, _ := strconv.Atoi(os.Getenv("OPEN_TSDB_LONG_TERM_DISCARD_RATIO"))

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     broker,
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics([]string{topic}, nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe: %s\n", err)
		os.Exit(1)
	}

	httpHeaders, _ := stringListToHeaderMap([]string{"Content-Encoding: gzip"})
	expectedStatuses, _ := commaDelimitedToIntList("204,400")

	bosunDatapointSender := NewHTTPSender([]string{openTsdbBosunUrl}, "/api/put", "POST", httpHeaders, expectedStatuses)
	bosunMetadataSender := NewHTTPSender([]string{openTsdbBosunUrl}, "/api/metadata/put", "POST", httpHeaders, expectedStatuses)
	openTsdbLegacySender := NewHTTPSender([]string{openTsdbLegacyUrl}, "/api/put", "POST", httpHeaders, expectedStatuses)
	openTsdbLongTermSender := NewHTTPSender([]string{openTsdbLongTermRetentionUrl}, "/api/put", "POST", httpHeaders, expectedStatuses)
	openTsdbShortTermSender := NewHTTPSender([]string{openTsdbShortTermRetentionUrl}, "/api/put", "POST", httpHeaders, expectedStatuses)
	bosunDiscardCounter := 0
	openTsdbLongTermDiscardCounter := 0
	openTsdbShortTermDiscardCounter := 0
	run := true

	for run {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:

				bosunDiscardCounter++
				openTsdbLongTermDiscardCounter++
				openTsdbShortTermDiscardCounter++

				if bosunDiscardCounter%openTsdbBosunDiscardRatio == 0 || openTsdbBosunDiscardRatio == 0 {
					bosunDiscardCounter = 0
					if err := bosunDatapointSender.RRSend(e.Value); err != nil {
						log.Printf("[BosunDatapoint] Error send data: %s\n", err)
					}
					if err := bosunMetadataSender.RRSend(e.Value); err != nil {
						log.Printf("[BosunMetadata] Error send data: %s\n", err)
					}
				}

				if openTsdbShortTermDiscardCounter%openTsdbShortTermRetentionDiscardRatio == 0 || openTsdbShortTermRetentionDiscardRatio == 0 {
					openTsdbShortTermDiscardCounter = 0
					if err := openTsdbShortTermSender.RRSend(e.Value); err != nil {
						log.Printf("[OpenTsdbShortTerm] Error send data: %s\n", err)
					}
				}

				if openTsdbLongTermDiscardCounter%openTsdbLongTermRetentionDiscardRatio == 0 || openTsdbLongTermRetentionDiscardRatio == 0 {
					openTsdbLongTermDiscardCounter = 0
					if err := openTsdbLegacySender.RRSend(e.Value); err != nil {
						log.Printf("[OpenTsdbShortTerm] Error send data: %s\n", err)
					}
					if err := openTsdbLongTermSender.RRSend(e.Value); err != nil {
						log.Printf("[OpenTsdbLongTerm] Error send data: %s\n", err)
					}
				}

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
				if e.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()
}

func commaDelimitedToStringList(s string) []string {
	list := strings.Split(s, ",")
	cleanList := make([]string, 0)
	for _, v := range list {
		c := strings.TrimSpace(v)
		if c != "" {
			cleanList = append(cleanList, c)
		}
	}
	return cleanList
}

func stringListToHeaderMap(l []string) (map[string][]string, error) {
	headers := make(map[string][]string, len(l))
	for _, h := range l {
		idx := strings.Index(h, ":")
		if idx == -1 {
			return nil, fmt.Errorf("Unable to parse header %s", h)
		}
		headers[h[0:idx]] = append(headers[h[0:idx]], strings.TrimSpace(h[idx+1:]))
	}

	return headers, nil
}

func commaDelimitedToIntList(s string) ([]int, error) {
	list := strings.Split(s, ",")
	intList := make([]int, len(list))
	for i, v := range list {
		var err error
		intList[i], err = strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return nil, err
		}
	}
	return intList, nil
}
