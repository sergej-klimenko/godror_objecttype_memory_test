
.PHONY: run
run:
	go run main.go

.PHONY: docker-run
docker-run:
	docker build -t godror_test .
	docker run -it --rm --name godror_test --network="host" \
	-e GODROR_TEST_DSN="$(GODROR_TEST_DSN)" \
	-e RUNS=$(RUNS) -e STEP=$(STEP) \
	godror_test

.PHONY: odpi-parse
odpi-parse:
	DPI_DEBUG_LEVEL=38 RUNS=1 STEP=1 go run main.go 2> odpi.log
	python3 odpi-parse.py odpi.log