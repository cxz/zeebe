.PHONY: install-deps
install-deps:
	go get -u github.com/golang/dep/cmd/dep
	go get github.com/onsi/ginkgo/ginkgo
	dep ensure

.PHONY: test
test:
	cd commands/ && go test -v && cd ../
	cd tests/integration/ && ginkgo && cd ../..
