import csv
import datetime
import math
import os
import time
import json
import requests
from kafka import KafkaProducer
from collections import deque

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "github_public_events"
POLL_SECONDS = 300
GITHUB_EVENTS_URL = "https://api.github.com/events"
STATE_FILE = ".seen_ids"
MAX_STATE_KEYS = 100

REPOS = {'anthropics/claude-code': [21.08, 17.08, 18.08, 22.0, 17.67, 16.92, 18.25, 14.58, 15.92, 104.42, 46.08, 15.33, 11.17, 11.83, 35.0, 12.83, 14.17, 15.17, 16.67, 19.92, 20.08, 20.42, 19.25, 18.67], 'facebook/react': [1.25, 2.0, 1.75, 1.88, 1.5, 1.75, 1.71, 2.75, 1.33, 1.33, 1.57, 1.75, 1.14, 1.0, 1.29, 1.67, 1.75, 1.33, 1.8, 1.86, 1.13, 2.5, 1.0, 1.6], 'ghostty-org/ghostty': [2.38, 2.11, 2.57, 2.22, 2.3, 1.86, 1.67, 1.8, 1.33, 1.5, 2.0, 1.67, 1.0, 1.0, 1.2, 2.0, 1.5, 1.67, 1.4, 1.63, 2.11, 2.3, 2.67, 2.0], 'godotengine/godot': [6.17, 2.64, 2.0, 1.55, 3.11, 2.0, 3.55, 4.56, 4.5, 7.67, 8.42, 9.0, 2.91, 3.91, 3.92, 6.83, 4.08, 3.5, 6.18, 6.67, 5.17, 10.25, 4.75, 4.75], 'kubernetes/kubernetes': [6.0, 5.0, 3.83, 5.33, 4.73, 3.4, 6.0, 9.33, 11.0, 7.75, 8.83, 10.33, 6.33, 7.17, 6.73, 6.33, 4.17, 5.83, 9.25, 10.27, 8.67, 6.75, 10.08, 5.0], 'llvm/llvm-project': [24.92, 21.42, 17.08, 15.67, 14.17, 17.0, 12.83, 11.0, 15.0, 18.17, 22.42, 23.67, 11.92, 14.08, 20.08, 24.33, 24.58, 23.92, 24.83, 32.83, 26.0, 33.33, 35.33, 28.08], 'microsoft/TypeScript': [2.0, 1.83, 1.0, 3.0, 2.17, 1.83, 2.11, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.2, 1.0, 1.0, 2.0, 2.8, 1.57, 1.6, 2.75, 2.5, 1.75], 'microsoft/vscode': [23.08, 14.17, 13.25, 20.67, 11.08, 11.5, 22.0, 11.83, 16.92, 16.58, 21.58, 37.08, 8.67, 7.42, 17.58, 18.08, 23.25, 17.5, 21.5, 26.83, 25.75, 25.5, 32.08, 30.33], 'nodejs/node': [5.33, 2.8, 1.38, 1.0, 2.89, 2.44, 2.89, 1.89, 3.09, 2.33, 1.63, 3.2, 3.0, 2.3, 2.91, 3.44, 2.3, 3.08, 4.82, 3.2, 2.4, 2.83, 3.33, 3.44], 'openclaw/openclaw': [81.92, 60.75, 89.92, 57.0, 75.0, 81.83, 99.75, 92.08, 95.08, 85.75, 73.92, 75.0, 63.0, 72.58, 71.17, 67.0, 65.67, 69.75, 54.17, 52.58, 61.25, 64.92, 71.75, 58.33], 'tensorflow/tensorflow': [4.75, 2.82, 3.6, 3.91, 6.0, 4.67, 3.27, 3.5, 6.0, 4.75, 6.58, 4.0, 3.4, 2.89, 4.5, 3.58, 2.09, 3.82, 3.42, 6.42, 5.36, 5.4, 5.25, 4.08], 'torvalds/linux': [1.13, 1.67, 1.27, 1.78, 1.38, 2.38, 1.8, 1.6, 2.0, 1.56, 2.25, 1.8, 1.4, 1.14, 2.22, 1.67, 1.25, 1.75, 1.45, 2.11, 2.25, 1.89, 1.63, 1.33]}


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

seen_ids = set()
seen_queue = deque(maxlen=MAX_STATE_KEYS)

def fetch_repo_events(repo, per_page):
    r = requests.get(
        f"https://api.github.com/repos/{repo}/events",
        params={"per_page": per_page},
        headers={"Accept": "application/vnd.github+json"},
        timeout=20
    )

    if r.status_code == 403 and r.headers.get("X-RateLimit-Remaining") == "0":
        raise RuntimeError("RATE_LIMIT", repo)

    r.raise_for_status()
    return r.json()


def load_seen_ids():
    if not os.path.exists(STATE_FILE):
        return
    with open(STATE_FILE, "r") as f:
        seen_ids.update(line.strip() for line in f.readlines()[-MAX_STATE_KEYS:])

def save_seen_ids():
    with open(STATE_FILE, "w") as f:
        for _id in list(seen_queue)[-MAX_STATE_KEYS:]:
            f.write(_id + "\n")

def clean_payload(event_type, payload):
    if not payload:
        return payload
    if event_type == "PushEvent":
        return {
            "ref": payload.get("ref"),
            "head": payload.get("head"),
            "before": payload.get("before"),
        }
    if event_type == "PullRequestEvent":
        action = payload.get("action")
        return {
            "action": payload.get("action"),
            "number": payload.get("number"),
            "head_id": payload.get("pull_request", {}).get("head", {}).get("repo", {}).get("id") if action == "merged" else None,
            "base_id": payload.get("pull_request", {}).get("base", {}).get("repo", {}).get("id") if action == "merged" else None,
            "head": payload.get("pull_request", {}).get("head", {}).get("ref"),
            "base": payload.get("pull_request", {}).get("base", {}).get("ref")
        }
    if event_type in ("IssuesEvent", "IssueCommentEvent"):
        return {
            "action": payload.get("action"),
            "number": payload.get("issue", {}).get("number")
        }
    return {
        "action": payload.get("action")
    }

def main():
    print("Starting GitHub Kafka producer...")
    load_seen_ids()
    num_failed = 0

    while True:
        try:
            current_hour = datetime.datetime.now(datetime.timezone.utc).hour
            total_events_sent = 0
            for repo, hourly_avgs in REPOS.items():
                avg_5min = hourly_avgs[current_hour]
                per_page = max(1, min(100, math.ceil(avg_5min * (1 + num_failed))))

                events = fetch_repo_events(repo, per_page)

                for event in events:
                    event_id = event.get("id")
                    if not event_id:
                        continue

                    repo = event.get("repo", {}).get("name")
                    if repo not in REPOS:
                        continue

                    if event_id in seen_ids:
                        continue

                    seen_ids.add(event_id)
                    seen_queue.append(event_id)
                    payload = event.get("payload")

                    payload = clean_payload(event.get("type"), payload)
                    
                    producer.send(
                        TOPIC,
                        key=event_id,
                        value={
                            "created_at": event.get("created_at"),
                            "type": event.get("type"),
                            "repo": event.get("repo", {}).get("name"),
                            "actor": event.get("actor", {}).get("login"),
                            "payload": json.dumps(payload) if payload is not None else None
                        }
                    )
                    total_events_sent += 1
                num_failed = 0 
            producer.flush()
            print(f"Sent {total_events_sent} events")

            if len(seen_ids) > 50_000:
                seen_ids.clear()
        except RuntimeError as e:
            if e.args and e.args[0] == "RATE_LIMIT":
                print("Rate limit exceeded. Sleeping for 5 minutes...")
                num_failed += 1
                time.sleep(300)
            else:
                print("Error:", e)
        except Exception as e:
            print("Error:", e)

        time.sleep(POLL_SECONDS)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.flush()
        producer.close()
        save_seen_ids()
        print("Producer closed cleanly.")

