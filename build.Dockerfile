FROM golang:1.8.3

RUN apt-get update && apt-get install -y git openssh-client && apt-get clean
RUN go get github.com/Masterminds/glide
RUN go get github.com/confluentinc/confluent-kafka-go/kafka
COPY . /go
