"""Exercise the real provider; Drift captures the resulting Kafka record."""
import json
import os
import sys
import urllib.request


def main():
    correlation_id = sys.argv[1]
    payload = {
        "id": correlation_id,
        "name": "Test product",
        "type": "BOOK",
        "version": "v1",
        "event": "CREATED",
        "price": 27.0,
    }
    url = os.environ.get("DRIFT_PROVIDER_URL", "http://localhost:8081").rstrip("/")
    request = urllib.request.Request(
        url + "/products",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "correlation-id": correlation_id},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        # Drift command hooks consume JSON on stdout.
        print(json.dumps({"status": response.status}))


if __name__ == "__main__":
    main()
