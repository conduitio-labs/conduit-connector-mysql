VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-mysql.version=${VERSION}'" -o conduit-connector-mysql cmd/connector/main.go

.PHONY: test
test: test-mysql test-mariadb down

.PHONY: test-internal
test-internal: up-database
	go test $(GOTEST_FLAGS) -v -race ./...

.PHONY: test-mysql
test-mysql:
	export DB_IMAGE=mysql:8.0.39 && make test-internal

.PHONY: test-mariadb
test-mariadb:
	export DB_IMAGE=mariadb:11.4 && make test-internal

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

.PHONY: up-database
up-database:
	docker compose -f test/docker-compose.yml up --quiet-pull -d db --wait

.PHONY: up-adminer
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

.PHONY: fmt
fmt:
	gofumpt -l -w .
	gci write --skip-generated  .
