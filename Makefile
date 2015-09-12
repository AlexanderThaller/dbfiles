coverage:
	rm -f coverage.out
	go test -coverprofile=coverage.out -short
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o=/tmp/coverage.html
