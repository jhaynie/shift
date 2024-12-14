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