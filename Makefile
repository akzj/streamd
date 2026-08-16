.PHONY: test test-race ha-prepare ha-up ha-test ha-logs ha-down test-ha

test:
	go test ./... -count=1

test-race:
	go test -race ./... -count=1

ha-prepare:
	./test/ha/compose.sh prepare

ha-up:
	./test/ha/compose.sh up

ha-test:
	./test/ha/compose.sh test

ha-logs:
	./test/ha/compose.sh logs

ha-down:
	./test/ha/compose.sh down

test-ha:
	@HA_PROJECT_NAME="streamd-ha-$$(id -u)-$$(date +%s)-$$$$" ./test/ha/compose.sh all
