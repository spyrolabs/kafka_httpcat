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

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3:]
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

	err = c.SubscribeTopics(topics, nil)

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to subscribe: %s\n", err)
		os.Exit(1)
	}

	httpHeaders, err := stringListToHeaderMap([]string{"Content-Encoding: gzip"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse headers: %s\n", err)
		os.Exit(1)
	}

	expectedStatuses, err := commaDelimitedToIntList("204,400")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to parse expected statuses: %s\n", err)
		os.Exit(1)
	}
	sender := NewHTTPSender([]string{"opentsdb.jumpy:4242", "opentsdb.slc-1f.jumpy:4242", "opentsdb.slc-1z.jumpy:4242"}, "/api/put", "POST", httpHeaders, expectedStatuses)

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
				if err := sender.RRSend(e.Value); err != nil {
					log.Printf("Error send data: %s\n", err)
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
