#!/bin/bash
# Kafka topic utilities

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

KAFKA_BIN="/opt/kafka/bin"
BOOTSTRAP="localhost:29092"

usage() {
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  list                    List all topics"
    echo "  describe <topic>        Describe a topic"
    echo "  dump <topic> [N]        Dump last N messages (default: all)"
    echo "  tail <topic>            Tail new messages (Ctrl+C to stop)"
    echo "  count <topic>           Count messages in topic"
    echo "  produce <topic>         Produce messages interactively"
    echo ""
    echo "Topics:"
    echo "  ifex.rpc.200            RPC messages"
    echo "  ifex.discovery.201      Discovery sync messages"
    echo "  ifex.scheduler.202      Scheduler sync messages"
    echo "  ifex.vehicle.enrichment Vehicle enrichment (compacted)"
    exit 1
}

case "${1:-}" in
    list)
        docker-compose exec -T kafka $KAFKA_BIN/kafka-topics.sh \
            --bootstrap-server $BOOTSTRAP --list
        ;;

    describe)
        [ -z "$2" ] && usage
        docker-compose exec -T kafka $KAFKA_BIN/kafka-topics.sh \
            --bootstrap-server $BOOTSTRAP --describe --topic "$2"
        ;;

    dump)
        [ -z "$2" ] && usage
        TOPIC="$2"
        MAX_MSGS="${3:-}"

        EXTRA_ARGS="--from-beginning --timeout-ms 5000"
        [ -n "$MAX_MSGS" ] && EXTRA_ARGS="$EXTRA_ARGS --max-messages $MAX_MSGS"

        docker-compose exec -T kafka $KAFKA_BIN/kafka-console-consumer.sh \
            --bootstrap-server $BOOTSTRAP \
            --topic "$TOPIC" \
            --property print.key=true \
            --property print.timestamp=true \
            $EXTRA_ARGS 2>/dev/null
        ;;

    tail)
        [ -z "$2" ] && usage
        echo "Tailing $2 (Ctrl+C to stop)..."
        docker-compose exec -T kafka $KAFKA_BIN/kafka-console-consumer.sh \
            --bootstrap-server $BOOTSTRAP \
            --topic "$2" \
            --property print.key=true \
            --property print.timestamp=true
        ;;

    count)
        [ -z "$2" ] && usage
        TOPIC="$2"

        # Count by consuming all messages
        COUNT=$(docker-compose exec -T kafka $KAFKA_BIN/kafka-console-consumer.sh \
            --bootstrap-server $BOOTSTRAP \
            --topic "$TOPIC" \
            --from-beginning \
            --timeout-ms 5000 2>/dev/null | wc -l)

        echo "$TOPIC: $COUNT messages"
        ;;

    produce)
        [ -z "$2" ] && usage
        echo "Enter messages (Ctrl+D to finish):"
        docker-compose exec -T kafka $KAFKA_BIN/kafka-console-producer.sh \
            --bootstrap-server $BOOTSTRAP \
            --topic "$2"
        ;;

    *)
        usage
        ;;
esac
