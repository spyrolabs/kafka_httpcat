.PHONY:	build push

IMAGE = infra/kafka_httpcat
VERSION = 1.0.0
TAG = $(VERSION)

compile:
	docker build -f build.Dockerfile -t $(IMAGE)_builder .
	docker run -e=VERSION=$(VERSION) -v=`pwd`/build:/build -ti $(IMAGE)_builder /go/build.sh

build:
	docker build -t jcr.io/$(IMAGE):$(TAG) .

push: build
	docker push jcr.io/$(IMAGE):$(TAG)
