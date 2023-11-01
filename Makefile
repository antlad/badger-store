
.PHONY: coverage
coverage:
	go test  -v -coverprofile=coverage.out $(PWD)/...  && go tool cover -html=coverage.out

.PHONY: test
test:
	go test -parallel=10 $(PWD)/... -coverprofile=cover.out -timeout=$(TEST_TIMEOUT)
