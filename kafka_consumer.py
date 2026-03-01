import json
import csv
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "github_public_events"
GROUP_ID = "github_csv_consumer"
OUTPUT_CSV = "github_events_stream.csv"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    group_id=GROUP_ID,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    key_deserializer=lambda k: k.decode("utf-8"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

def main():
    print("Starting Kafka consumer...")

    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        if f.tell() == 0:
            writer.writerow([
                "created_at",
                "event_type",
                "repo",
                "actor",
                "payload",
            ])

        for msg in consumer:
            event = msg.value
            writer.writerow([
                event.get("created_at"),
                event.get("type"),
                event.get("repo"),
                event.get("actor"),
                event.get("payload")
            ])


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        consumer.close()
