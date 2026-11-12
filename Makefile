.PHONY: check fmt-check vet test build

check: fmt-check vet test

fmt-check:
	@files="$$(gofmt -l .)"; if [ -n "$$files" ]; then echo "gofmt needed:"; echo "$$files"; exit 1; fi

vet:
	go vet ./...

# Three shuffled runs with the race detector, then one serial run.
test:
	go test -count=1 -race -shuffle=11 ./...
	go test -count=1 -race -shuffle=222 ./...
	go test -count=1 -race -shuffle=3333 ./...
	go test -count=1 -p 1 -parallel 1 -shuffle=44444 ./...

build:
	go build -o bin/wingpost ./cmd/wingpost
