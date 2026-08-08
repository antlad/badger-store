
.PHONY: coverage
coverage:
	go test  -v -coverprofile=coverage.out $(PWD)/...  && go tool cover -html=coverage.out

.PHONY: test
test:
	go test -parallel=10 $(PWD)/... -coverprofile=cover.out

.PHONY: lint
lint:
	golangci-lint run ./...

.PHONY: lint-fix
lint-fix:
	golangci-lint run --fix ./...

