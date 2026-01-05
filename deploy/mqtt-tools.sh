#!/bin/bash
# MQTT utilities for testing

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

MQTT_HOST="${MQTT_HOST:-localhost}"
MQTT_PORT="${MQTT_PORT:-1883}"

usage() {
    echo "Usage: $0 <command> [args]"
    echo ""
    echo "Commands:"
    echo "  sub [topic]         Subscribe to topic (default: #)"
    echo "  sub-v2c             Subscribe to all v2c/# messages"
    echo "  sub-c2v             Subscribe to all c2v/# messages"
    echo "  pub <topic> <msg>   Publish message to topic"
    echo "  status              Check MQTT broker status"
    echo ""
    echo "Environment:"
    echo "  MQTT_HOST=$MQTT_HOST"
    echo "  MQTT_PORT=$MQTT_PORT"
    exit 1
}

check_mosquitto_clients() {
    if ! command -v mosquitto_sub &> /dev/null; then
        echo "Error: mosquitto-clients not installed"
        echo "Install with: sudo apt-get install mosquitto-clients"
        exit 1
    fi
}

case "${1:-}" in
    sub)
        check_mosquitto_clients
        TOPIC="${2:-#}"
        echo "Subscribing to $TOPIC on $MQTT_HOST:$MQTT_PORT (Ctrl+C to stop)..."
        mosquitto_sub -h "$MQTT_HOST" -p "$MQTT_PORT" -t "$TOPIC" -v
        ;;

    sub-v2c)
        check_mosquitto_clients
        echo "Subscribing to v2c/# on $MQTT_HOST:$MQTT_PORT (Ctrl+C to stop)..."
        mosquitto_sub -h "$MQTT_HOST" -p "$MQTT_PORT" -t "v2c/#" -v
        ;;

    sub-c2v)
        check_mosquitto_clients
        echo "Subscribing to c2v/# on $MQTT_HOST:$MQTT_PORT (Ctrl+C to stop)..."
        mosquitto_sub -h "$MQTT_HOST" -p "$MQTT_PORT" -t "c2v/#" -v
        ;;

    pub)
        check_mosquitto_clients
        [ -z "$2" ] || [ -z "$3" ] && usage
        TOPIC="$2"
        MSG="$3"
        echo "Publishing to $TOPIC..."
        mosquitto_pub -h "$MQTT_HOST" -p "$MQTT_PORT" -t "$TOPIC" -m "$MSG"
        echo "Done"
        ;;

    status)
        echo "Checking MQTT broker at $MQTT_HOST:$MQTT_PORT..."
        if nc -z "$MQTT_HOST" "$MQTT_PORT" 2>/dev/null; then
            echo "MQTT broker is reachable"

            # Try to get $SYS info if available
            if command -v mosquitto_sub &> /dev/null; then
                echo ""
                echo "Broker stats (5 second sample):"
                timeout 5 mosquitto_sub -h "$MQTT_HOST" -p "$MQTT_PORT" \
                    -t '$SYS/broker/clients/#' -v 2>/dev/null || true
            fi
        else
            echo "MQTT broker is not reachable"
            exit 1
        fi
        ;;

    *)
        usage
        ;;
esac
