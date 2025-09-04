VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-mysql.version=${VERSION}'" -o conduit-connector-mysql cmd/connector/main.go

.PHONY: test
test: up
	go test $(GOTEST_FLAGS) -v -race ./...

.PHONY: generate
generate:
	go generate ./...
	conn-sdk-cli readmegen -w

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy

.PHONY: lint
lint:
	golangci-lint run

.PHONY: up
up:
	docker compose -f test/docker-compose.yml up mysql --wait

.PHONY: up-mariadb
up-mariadb:
	docker compose -f test/docker-compose.yml up mariadb --wait

.PHONY: down
down:
	docker compose -f test/docker-compose.yml down -v --remove-orphans

.PHONY: connect
connect:
	docker exec -it mysql_db mysql -u root -p'meroxaadmin' meroxadb

.PHONY: connect-mariadb
connect-mariadb:
	docker exec -it mariadb_db mariadb -u root -p'meroxaadmin' meroxadb

.PHONY: fmt
fmt:
	gofumpt -l -w .
	gci write --skip-generated  .
