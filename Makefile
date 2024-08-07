VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-mysql.version=${VERSION}'" -o conduit-connector-mysql cmd/connector/main.go

.PHONY: test-integration
test-integration: up-database
	go test -v -race ./...; ret=$$?; \
		docker compose -f test/docker-compose.yml down; \
		exit $$ret

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: up-database
up-database:
	docker compose -f test/docker-compose.yml up --quiet-pull -d db --wait

.PHONY: up-database
up-adminer:
	docker compose -f test/docker-compose.yml up --quiet-pull -d adminer --wait

.PHONY: up
up:
	docker compose -f test/docker-compose.yml up --wait

.PHONY: down
down:
	docker compose -f test/docker-compose.yml down -v --remove-orphans

.PHONY: connect
connect:
	docker exec -it mysql_db mysql -u root -p'meroxaadmin' meroxadb