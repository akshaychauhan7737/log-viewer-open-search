import os
import time
import zipfile
import json
import requests

OPENSEARCH_URL = os.getenv("OPENSEARCH_URL", "https://opensearch:9200")
INDEX_NAME = os.getenv("INDEX_NAME", "logs-zip-demo")
LOGS_DIR = os.getenv("LOGS_DIR", "/logs")
USERNAME = os.getenv("OPENSEARCH_USERNAME", "admin")
PASSWORD = os.getenv("OPENSEARCH_PASSWORD")
BATCH_SIZE = 1000
RETRY_COUNT = 30
RETRY_DELAY = 2

session = requests.Session()

session.auth = (USERNAME, PASSWORD)
session.verify = False  # because cluster uses self-signed certs in demo mode


def wait_for_opensearch():
    """Wait until OpenSearch is reachable."""
    for i in range(RETRY_COUNT):
        try:
            resp = session.get(OPENSEARCH_URL)
            if resp.status_code == 200:
                print("✅ OpenSearch is up.")
                return
        except Exception as e:
            print(f"OpenSearch not ready yet ({e}), retrying...")
        time.sleep(RETRY_DELAY)
    raise RuntimeError("❌ OpenSearch did not become ready in time.")


def ensure_index():
    """Create index if it does not exist."""
    resp = session.get(f"{OPENSEARCH_URL}/{INDEX_NAME}")
    if resp.status_code == 200:
        print(f"Index '{INDEX_NAME}' already exists.")
        return

    print(f"Creating index '{INDEX_NAME}'...")
    payload = {
        "mappings": {
            "properties": {
                "file": {"type": "keyword"},
                "source_zip": {"type": "keyword"},
                "message": {"type": "text"}
            }
        }
    }
    r = session.put(
        f"{OPENSEARCH_URL}/{INDEX_NAME}",
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
    )
    r.raise_for_status()
    print("Index created.")


def send_bulk(actions):
    """Send bulk indexing request."""
    if not actions:
        return
    data = "\n".join(actions) + "\n"
    resp = session.post(
        f"{OPENSEARCH_URL}/_bulk",
        data=data,
        headers={"Content-Type": "application/x-ndjson"},
    )
    resp.raise_for_status()
    result = resp.json()
    if result.get("errors"):
        print("⚠️ Bulk request had errors.")
    else:
        print(f"✅ Indexed {len(actions) // 2} documents")


def process_zip(zip_path):
    """Process a single zip file and index its log lines."""
    print(f"Processing ZIP: {zip_path}")
    source_zip = os.path.basename(zip_path)
    try:
        zf = zipfile.ZipFile(zip_path)
    except Exception as e:
        print(f"❌ Failed to open ZIP {zip_path}: {e}")
        return

    batch = []

    for name in zf.namelist():
        # Adjust these extensions as needed
        if not (name.endswith(".log") or name.endswith(".txt") or name.endswith(".json")):
            print(f"Skipping non-log file in ZIP: {name}")
            continue

        print(f"  -> File inside ZIP: {name}")
        with zf.open(name) as f:
            for raw in f:
                line = raw.decode("utf-8", "ignore").rstrip("\n")
                if not line:
                    continue

                # Try to parse as JSON; fallback to plain message
                doc_data = {
                    "file": name,
                    "source_zip": source_zip,
                }
                try:
                    parsed = json.loads(line)
                    if isinstance(parsed, dict):
                        doc_data.update(parsed)
                    else:
                        doc_data["message"] = line
                except json.JSONDecodeError:
                    doc_data["message"] = line

                meta = json.dumps({"index": {"_index": INDEX_NAME}})
                doc = json.dumps(doc_data)

                batch.append(meta)
                batch.append(doc)

                if len(batch) >= BATCH_SIZE * 2:
                    send_bulk(batch)
                    batch = []

    if batch:
        send_bulk(batch)

    print(f"🎉 Finished processing ZIP: {zip_path}")


def scan_and_process():
    """Scan LOGS_DIR for .zip files and process them."""
    if not os.path.isdir(LOGS_DIR):
        print(f"❌ LOGS_DIR does not exist or is not a directory: {LOGS_DIR}")
        return

    found_any = False
    for root, dirs, files in os.walk(LOGS_DIR):
        for fname in files:
            if fname.lower().endswith(".zip"):
                found_any = True
                full_path = os.path.join(root, fname)
                process_zip(full_path)

    if not found_any:
        print(f"⚠️ No .zip files found in {LOGS_DIR}")


def main():
    print(f"Connecting to OpenSearch at {OPENSEARCH_URL}")
    wait_for_opensearch()
    ensure_index()
    print(f"Scanning folder for ZIP logs: {LOGS_DIR}")
    scan_and_process()
    print("✅ All done. Exiting.")


if __name__ == "__main__":
    main()
