#!/usr/bin/env bash
#
# Deletes one Kafka topic, or all application topics, and reports on it.
# Deleted topics come back empty next time their owning Spring Boot app starts,
# via its NewTopic bean (see KafkaTopicConfig in transaction-generator / fraud-streams-app).
#
# Usage:
#   ./scripts/empty-topic.sh <topic-name>   Delete a single topic
#   ./scripts/empty-topic.sh --all          Delete every application topic. Excludes:
#                                              - Kafka/Schema Registry internals (__consumer_offsets, _schemas, leading underscore)
#                                              - Kafka Streams-managed internal topics (*-changelog, *-repartition)
#                                            The latter back live state stores — deleting them out from under
#                                            a running Streams app can crash its StreamThread. Use the dedicated
#                                            kafka-streams-application-reset.sh tool (app stopped) to reset those safely.

set -euo pipefail

BOOTSTRAP_SERVER="localhost:9092"
KAFKA_BIN="/opt/kafka/bin"

list_topics() {
  docker exec kafka "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --list
}

delete_topic() {
  local topic="$1"
  echo "Deleting topic '$topic'..."
  docker exec kafka "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_SERVER" --delete --topic "$topic"
}

if [ $# -ne 1 ]; then
  echo "Usage: $0 <topic-name>"
  echo "       $0 --all"
  exit 1
fi

if [ "$1" == "--all" ]; then
  # Exclude Kafka/Schema Registry internals (leading underscore): __consumer_offsets, _schemas, etc.
  # Exclude Kafka Streams-managed internal topics: <application-id>-*-changelog, <application-id>-*-repartition.
  # Deleting either category can break offset tracking, Schema Registry storage, or a running Streams app's state.
  APP_TOPICS=$(list_topics | grep -v '^_' | grep -Ev -- '-changelog$|-repartition$' || true)

  if [ -z "$APP_TOPICS" ]; then
    echo "No application topics found — nothing to delete."
    exit 0
  fi

  echo "This will delete the following topics:"
  echo "$APP_TOPICS" | sed 's/^/  - /'
  echo ""
  read -r -p "Are you sure? [y/N] " CONFIRM
  if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "Aborted."
    exit 0
  fi

  while IFS= read -r topic; do
    delete_topic "$topic"
  done <<< "$APP_TOPICS"

  echo ""
  echo "Done. Deleted topics will be recreated empty next time their owning apps start."
else
  TOPIC="$1"

  echo "Checking if topic '$TOPIC' exists..."
  if ! list_topics | grep -qx "$TOPIC"; then
    echo "Topic '$TOPIC' does not exist — nothing to delete."
    exit 0
  fi

  delete_topic "$TOPIC"
  echo "Done. '$TOPIC' will be recreated empty next time its owning app starts (via its NewTopic bean)."
fi

echo "Remaining topics:"
list_topics
