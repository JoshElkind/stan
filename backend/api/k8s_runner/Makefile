IMAGE=joshelkind/algo-runner
TAG=latest

build:
	docker build -t $(IMAGE):$(TAG) .

push:
	docker push $(IMAGE):$(TAG)

all: build push
