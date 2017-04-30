all: build

build: build-didgen

build-didgen:
	go build -o ./didgen ./didgen.go

test:
	go test --race ./...

clean:
	go clean -i ./...
	@rm -rf ./didgen