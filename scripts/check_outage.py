"""Exercise a real Redis outage using this project's already running demo API.

Run from the repository root with Docker Compose and host Python available.
The API must start with zero counters in cache_aside mode. Redis is restarted
in finally, including when an assertion or HTTP request fails.
"""
import json
import subprocess
import time
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.request import Request, urlopen


def request(path, patch=None):
    data = None if patch is None else json.dumps(patch).encode()
    req = Request("http://127.0.0.1:8000" + path, data=data,
                  headers={"Content-Type": "application/json"},
                  method="GET" if patch is None else "PATCH")
    with urlopen(req, timeout=10) as response:
        return json.load(response)


def compose(*args):
    subprocess.run(["docker", "compose", *args], check=True,
                   stdout=subprocess.DEVNULL)


def main():
    for attempt in range(50):
        try:
            health = request("/health")
            break
        except (URLError, OSError):
            if attempt == 49:
                raise
            time.sleep(0.1)
    assert health["mode"] == "cache_aside"
    before = request("/debug/stats")
    assert before["source_reads"] == before["source_writes"] == 0
    warm = request("/products/1")
    try:
        compose("stop", "redis")
        during_get = request("/products/1")
        during_patch = request("/products/1", {"price_kopecks": 61000})
        during_stats = request("/debug/stats")
    finally:
        compose("start", "redis")
    for attempt in range(50):
        ping = subprocess.run(
            ["docker", "compose", "exec", "-T", "redis", "redis-cli", "ping"],
            capture_output=True, text=True, timeout=10,
        )
        if ping.returncode == 0 and ping.stdout.strip() == "PONG":
            break
        if attempt == 49:
            raise RuntimeError("Redis did not recover")
        time.sleep(0.1)
    recovered = request("/products/1")
    repeated = request("/products/1")
    after = request("/debug/stats")
    assert during_patch["revision"] == recovered["revision"] == 2
    assert recovered["price_kopecks"] == repeated["price_kopecks"] == 61000
    assert after["source_reads"] == 3
    print(json.dumps({
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "scenario": "redis_outage", "get_requests": 4,
        "before": before, "during": during_stats, "after": after,
        "warm": warm, "get_during_outage": during_get,
        "patch_during_outage": during_patch, "get_after_recovery": recovered,
        "repeated_get": repeated,
        "source_reads_delta": after["source_reads"] - before["source_reads"],
        "source_writes_delta": after["source_writes"] - before["source_writes"],
        "note": "Redis persistence is disabled; retained-key recovery is tested by B5.",
    }, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
