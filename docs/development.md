# Development

```sh
make test       # go test with coverage
make coverage   # coverage + HTML report
make lint       # golangci-lint v2 (config in .golangci.yml)
make lint-fix   # golangci-lint with autofix
```

Run the race detector and the examples before sending a change:

```sh
go test -race ./...
go run ./examples/basic
go run ./examples/migration
```

CI (GitHub Actions, `.github/workflows/ci.yml`) runs `go build`, `go vet`, `go test -race` on the
go.mod floor and current stable Go, plus golangci-lint, on every push and pull request.

## Benchmarks

```sh
go test -run '^$' -bench BenchmarkRead -benchmem ./
```

## Regenerating the test FlatBuffers types

Requires `flatc`:

```sh
cd test && go generate ./...
```
