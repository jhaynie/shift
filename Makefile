.PHONY: types tidy format test e2e all

all: tidy format test

types:
	@go-jsonschema -p schema ./schema.json > internal/schema/types.go

tidy:
	@go mod tidy

format:
	@go fmt ./...

test:
	@go test -v ./...

e2e:
	@go run -tags e2e . e2e --log-level=trace