COVERPROFILE ?= cover.out
COVER_THRESHOLD ?= 5

.PHONY: all build test lint fmt vet coverprofile cover cover-html cover-check clean

all: build test lint

build:
	go build ./...

test:
	go test -race ./...

lint:
	golangci-lint run

fmt:
	go fmt ./...

vet:
	go vet ./...

# Run the full test suite with coverage.
coverprofile:
	go test -covermode=atomic -coverprofile=$(COVERPROFILE) ./...

cover: coverprofile
	@go tool cover -func=$(COVERPROFILE)

cover-html: coverprofile
	go tool cover -html=$(COVERPROFILE)

cover-check: coverprofile
	@total=$$(go tool cover -func=$(COVERPROFILE) | awk '/^total:/ {print $$NF}' | tr -d '%'); \
	echo "total coverage: $$total% (threshold: $(COVER_THRESHOLD)%)"; \
	awk -v t="$$total" -v min=$(COVER_THRESHOLD) 'BEGIN { if (t+0 < min+0) { printf "coverage %s%% is below threshold %s%%\n", t, min; exit 1 } }'

clean:
	rm -f $(COVERPROFILE) $(COVERPROFILE)
