.PHONY: test test-race ha-prepare ha-up ha-test ha-logs ha-down test-ha test-ha-repeat

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

test-ha-repeat:
	@count="$${HA_REPEAT:-10}"; \
	case "$$count" in ''|*[!0-9]*) echo "HA_REPEAT must be a positive integer" >&2; exit 2;; esac; \
	[ "$$count" -gt 0 ] || { echo "HA_REPEAT must be greater than zero" >&2; exit 2; }; \
	index=1; \
	while [ "$$index" -le "$$count" ]; do \
		echo "HA acceptance run $$index/$$count"; \
		$(MAKE) --no-print-directory test-ha || exit $$?; \
		index=$$((index + 1)); \
	done
