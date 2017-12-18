BINARIES=exsrv

all: $(BINARIES)

exsrv: redrpc/*.go commands/exsrv/*.go
	./scripts/go build -o "$@" github.com/Codility/redis-rpc/go/commands/exsrv

.PHONY: clean
clean:
	rm -rf $(BINARIES) .gopath

.PHONY: test
test: goimports govet
	./scripts/go test -v github.com/Codility/redis-rpc/go/redrpc

.PHONY: govet
govet:
	find . -name '*.go' -and -not -path './vendor/*' -and -not -path './.gopath/*' | \
		while read f; do echo `dirname "$$f"`; done | uniq | xargs ./scripts/go tool vet

.PHONY: goimports
goimports:
	./scripts/go get golang.org/x/tools/cmd/goimports
	@echo "Running goimports..."
	@output=$$(find . -name '*.go' -and -not -path './vendor/*' -and -not -path './.gopath/*' | xargs ./.gopath/bin/goimports -d) && \
		if ! [ -z "$$output" ]; then \
			echo "$$output"; \
			echo "goimports failed!"; \
			exit 1; \
		fi
	@echo "goimports passed"
