.DEFAULT_GOAL := test

fmt:
	go fmt ./...
.PHONY:fmt

lint: fmt
	# flags allow revive to pick up uncommented exported funcs
	golangci-lint run --exclude-use-default=false --enable=revive
.PHONY:lint

vet: fmt 
	go vet ./...
.PHONY:vet

test: vet
	docker compose -f compose.test.yml down
	docker compose -f compose.test.yml up --detach
	go test -v ./...
	docker compose -f compose.test.yml down

clean:
	go clean

