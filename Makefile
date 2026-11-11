.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -trimpath -ldflags "-X 'github.com/conduitio/conduit-connector-elasticsearch.version=${VERSION}'" -o conduit-connector-elasticsearch cmd/connector/main.go

ES_VERSIONS := 5 6 7 8

# Run unit tests, then the integration tests against every supported
# Elasticsearch version. Containers are always stopped (and their logs dumped
# when they fail to become healthy), so a failing job is diagnosable from CI logs.
test: test-unit $(addprefix test-es-,$(ES_VERSIONS))

# Tests that do not require Docker services to be running
.PHONY: test-unit
test-unit:
	go test $(GOTEST_FLAGS) -race $$(go list ./... | grep -Fv '/test/v')

# Elasticsearch integration tests (one version), e.g. `make test-es-8`
test-es-%:
	@echo "# Elasticsearch v$*"
	docker compose -f test/docker-compose.v$*.yml -p test-v$* up --quiet-pull -d --wait \
		|| { docker compose -f test/docker-compose.v$*.yml -p test-v$* logs --no-color; \
		     docker compose -f test/docker-compose.v$*.yml -p test-v$* down -v; exit 1; }
	go test $(GOTEST_FLAGS) -race ./test/v$*; ret=$$?; \
		docker compose -f test/docker-compose.v$*.yml -p test-v$* down -v; \
		exit $$ret

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy

.PHONY: generate
generate:
	go generate ./...

.PHONY: fmt
fmt:
	gofumpt -l -w .

.PHONY: lint
lint:
	golangci-lint run
	