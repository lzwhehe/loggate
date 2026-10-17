.PHONY: build test vet bench-quick bench-full

BINS := loggate mocksink loadgen reconcile faultbench

build:
	@mkdir -p bin
	@for c in $(BINS); do go build -o bin/$$c ./cmd/$$c || exit 1; done

vet:
	gofmt -l . | (! grep .)
	go vet ./...

test:
	go test ./... -count=1

# Smoke run (200 eps, ~60 s per scenario, 1 repetition).
bench-quick:
	PROFILE=quick scripts/faultbench.sh

# Final numbers (2000 eps, 5 min per scenario, 5 repetitions). Linux.
bench-full:
	PROFILE=full REPS=5 scripts/faultbench.sh
