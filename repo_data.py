import requests
import csv
import time

REPOS = [
    "torvalds/linux",
    "tensorflow/tensorflow",
    "facebook/react",
    "microsoft/vscode",
    "kubernetes/kubernetes",
    "microsoft/TypeScript",
    "nodejs/node",
    "anthropics/claude-code",
    "openclaw/openclaw",
    "godotengine/godot",
    "ghostty-org/ghostty",
    "llvm/llvm-project"
]

OUTPUT_CSV = "repos_metadata.csv"
BASE_URL = "https://api.github.com/repos"

HEADERS = {
    "Accept": "application/vnd.github+json"
}

def fetch_repo(repo):
    r = requests.get(f"{BASE_URL}/{repo}", headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "repo",
        "created_at",
        "updated_at",
        "pushed_at",
        "stars",
        "watchers",
        "forks",
        "open_issues",
        "language",
        "license"
    ])

    for repo in REPOS:
        print("Fetching:", repo)
        data = fetch_repo(repo)
        writer.writerow([
            repo,
            data.get("created_at"),
            data.get("updated_at"),
            data.get("pushed_at"),
            data.get("stargazers_count"),
            data.get("watchers_count"),
            data.get("forks_count"),
            data.get("open_issues_count"),
            data.get("language"),
            data.get("license", {}).get("name") if data.get("license") else None
        ])

        time.sleep(1)

print("Done.")
