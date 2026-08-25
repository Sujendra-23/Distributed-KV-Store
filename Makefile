.PHONY: proto build test run-local docker-build clean

proto:
	protoc --go_out=. --go_opt=paths=source_relative \
	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
	       proto/kv.proto

build:
	go build -o bin/node ./cmd/node
	go build -o bin/coordinator ./cmd/coordinator
	go build -o bin/client ./cmd/client

test:
	go test ./... -v

# Runs a 3-node cluster + coordinator directly on the host, useful for
# quick local testing without Docker.
run-local: build
	./bin/node -addr=:7001 & \
	./bin/node -addr=:7002 & \
	./bin/node -addr=:7003 & \
	sleep 1; \
	./bin/coordinator -http=:8080 -nodes=localhost:7001,localhost:7002,localhost:7003

docker-build:
	docker build -f Dockerfile.node -t kvstore-node:latest .
	docker build -f Dockerfile.coordinator -t kvstore-coordinator:latest .

clean:
	rm -rf bin/
