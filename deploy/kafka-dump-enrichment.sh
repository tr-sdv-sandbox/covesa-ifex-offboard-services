#!/bin/bash
# Dump all enrichment data from Kafka compacted topic
# Shows latest value for each vehicle_id key

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

TOPIC="${1:-ifex.vehicle.enrichment}"
FORMAT="${2:-json}"  # json, keys, or count

KAFKA_CONSUMER="/opt/kafka/bin/kafka-console-consumer.sh"

case "$FORMAT" in
    keys)
        # Just the keys (vehicle IDs)
        echo "=== Vehicle IDs in Kafka ==="
        docker-compose exec -T kafka $KAFKA_CONSUMER \
            --bootstrap-server localhost:29092 \
            --topic "$TOPIC" \
            --from-beginning \
            --timeout-ms 5000 \
            --property print.key=true \
            --property print.value=false \
            2>/dev/null | sort
        ;;

    count)
        # Count messages
        COUNT=$(docker-compose exec -T kafka $KAFKA_CONSUMER \
            --bootstrap-server localhost:29092 \
            --topic "$TOPIC" \
            --from-beginning \
            --timeout-ms 5000 \
            2>/dev/null | wc -l)
        echo "Enrichment records in Kafka: $COUNT"
        ;;

    json|*)
        # JSON output with keys
        docker-compose exec -T kafka $KAFKA_CONSUMER \
            --bootstrap-server localhost:29092 \
            --topic "$TOPIC" \
            --from-beginning \
            --timeout-ms 5000 \
            2>/dev/null
        ;;
esac
