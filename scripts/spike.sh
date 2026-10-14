#!/usr/bin/env bash
#
# Runs the Hot Sale spike end to end several times, with and without another client saturating the inventory service.
#
set -euo pipefail

MACHINE=${MACHINE:?set MACHINE to a description of the machine this runs on}
SCENARIOS=${SCENARIOS:-"spike spike-with-outside-load"}
RUNS=${RUNS:-5}
SCHEDULE=${SCHEDULE:-43:1m,430:2m,43:5m}
LOAD_AT=${LOAD_AT:-110}
LOAD_RATE=${LOAD_RATE:-150}
LOAD_FOR=${LOAD_FOR:-20s}
DEDUP_MAX=${DEDUP_MAX:-200000}

FILE=${FILE:-data/eventos_hotsale.jsonl}
INVENTORY_URL=${INVENTORY_URL:-http://localhost:8090}
REDIS_CONTAINER=${REDIS_CONTAINER:-go04-redis}
REDIS_ADDR=${REDIS_ADDR:-localhost:6379}
STREAM=${STREAM:-hotsale:spike}
GROUP=${GROUP:-spike}
OUT_DIR=${OUT_DIR:-data/spike}
LOG_DIR=${LOG_DIR:-bin/logs}

redis() {
	docker exec "$REDIS_CONTAINER" redis-cli -e "$@"
}

events_in_schedule() {
	awk -v schedule="$1" 'BEGIN {
		n = split(schedule, segments, ",")
		for (i = 1; i <= n; i++) {
			if (split(segments[i], part, ":") != 2) exit 1
			unit = substr(part[2], length(part[2]))
			amount = substr(part[2], 1, length(part[2]) - 1)
			if (part[1] !~ /^[0-9]+$/ || amount !~ /^[0-9]+$/) exit 1
			if (unit == "m") amount *= 60
			else if (unit != "s") exit 1
			total += part[1] * amount
		}
		print total
	}'
}

mkdir -p bin "$OUT_DIR" "$LOG_DIR"

for attempt in $(seq 1 60); do
	if [ "$(redis ping 2>/dev/null | tr -d '\r')" = "PONG" ]; then
		break
	fi
	if [ "$attempt" -eq 60 ]; then
		echo "redis in $REDIS_CONTAINER never became ready" >&2
		exit 1
	fi
	sleep 1
done

if ! curl -fsS "$INVENTORY_URL/stats" >/dev/null; then
	echo "the inventory service does not answer at $INVENTORY_URL; start it with: python3 data/inventory_service.py 2>/tmp/inventory-service.log" >&2
	exit 1
fi

if ! LINES_TO_SEND=$(events_in_schedule "$SCHEDULE"); then
	echo "SCHEDULE must be rate:duration segments in whole seconds or minutes, such as 43:1m,430:2m,43:5m" >&2
	exit 1
fi
EXPECTED_UNIQUE=$(head -n "$LINES_TO_SEND" "$FILE" | cut -d'"' -f4 | sort -u | wc -l)
EXPECTED_RESERVATIONS=$(head -n "$LINES_TO_SEND" "$FILE" | awk -F'"' '!seen[$4]++ && $8 == "inventory_reserved"' | wc -l)
echo "schedule $SCHEDULE: $LINES_TO_SEND lines, $EXPECTED_UNIQUE unique events, $EXPECTED_RESERVATIONS unique reservations"

go build -o bin/producer ./cmd/producer
go build -o bin/consumer ./cmd/consumer
go build -o bin/svcprobe ./cmd/svcprobe

failed=0
for scenario in $SCENARIOS; do
	for run in $(seq 1 "$RUNS"); do
		base="$OUT_DIR/$scenario-run$run"
		log="$LOG_DIR/$scenario-run$run"

		curl -fsS -X POST "$INVENTORY_URL/reset" >/dev/null
		redis DEL "$STREAM" >/dev/null

		./bin/consumer -redis-addr "$REDIS_ADDR" -stream "$STREAM" -group "$GROUP" -inventory-url "$INVENTORY_URL" \
			-dedup-max "$DEDUP_MAX" -expect-unique "$EXPECTED_UNIQUE" -out "$base-consumer.json" >"$log-consumer.log" 2>&1 &
		consumer_pid=$!
		sleep 1

		load_pid=""
		if [ "$scenario" = "spike-with-outside-load" ]; then
			(
				sleep "$LOAD_AT"
				./bin/svcprobe -mode load -url "$INVENTORY_URL" -load-rate "$LOAD_RATE" -load-for "$LOAD_FOR" \
					-machine "$MACHINE" -out "$base-load.json"
			) >"$log-load.log" 2>&1 &
			load_pid=$!
		fi

		producer_status=0
		./bin/producer -redis-addr "$REDIS_ADDR" -file "$FILE" -stream "$STREAM" -schedule "$SCHEDULE" \
			-out "$base-producer.json" >"$log-producer.log" 2>&1 || producer_status=$?

		consumer_status=0
		wait "$consumer_pid" || consumer_status=$?
		load_status=0
		if [ -n "$load_pid" ]; then
			wait "$load_pid" || load_status=$?
		fi

		service=$(curl -fsS "$INVENTORY_URL/stats")
		stream_length=$(redis XLEN "$STREAM" | tr -d '\r')
		stream_pending=$(redis XPENDING "$STREAM" "$GROUP" | head -n 1 | tr -d '\r')
		printf '{"scenario": "%s", "run": %d, "machine": "%s", "schedule": "%s", "lines_sent": %d, "expected_unique_events": %d, "expected_reservations": %d, "producer_exit_status": %d, "consumer_exit_status": %d, "load_exit_status": %d, "stream_length": %d, "stream_pending": %d, "service": %s}\n' \
			"$scenario" "$run" "$MACHINE" "$SCHEDULE" "$LINES_TO_SEND" "$EXPECTED_UNIQUE" "$EXPECTED_RESERVATIONS" \
			"$producer_status" "$consumer_status" "$load_status" "$stream_length" "$stream_pending" "$service" >"$base-checks.json"

		if [ "$producer_status" -ne 0 ] || [ "$consumer_status" -ne 0 ] || [ "$load_status" -ne 0 ]; then
			failed=$((failed + 1))
		fi

		echo
		echo "$scenario run $run: producer exit $producer_status, consumer exit $consumer_status, stream $stream_length long with $stream_pending pending, service $service"
		grep -E '^(count|breaker|backpressure) ' "$log-consumer.log" | sed 's/^/  /' || true
	done
done

redis DEL "$STREAM" >/dev/null
echo
echo "reports in $OUT_DIR, logs in $LOG_DIR"
if [ "$failed" -ne 0 ]; then
	echo "$failed runs ended with a non-zero exit status" >&2
	exit 1
fi
