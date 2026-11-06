"""Measure the running demo API; restart it before each independent experiment.

Examples (TTL must match the API environment):
    python scripts/measure.py sequential --ttl 60
    python scripts/measure.py concurrent --ttl 60
    python scripts/measure.py swr --ttl 1
"""
import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.request import Request, urlopen


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("scenario", choices=["sequential", "concurrent", "expiry", "patch", "swr"])
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--ttl", type=float, default=5)
    args = parser.parse_args()

    def request(path, patch=None):
        data = None if patch is None else json.dumps(patch).encode()
        req = Request(args.url + path, data=data,
                      headers={"Content-Type": "application/json"},
                      method="GET" if patch is None else "PATCH")
        start = time.perf_counter()
        with urlopen(req, timeout=10) as response:
            body = json.load(response)
        return {"body": body, "milliseconds": round((time.perf_counter() - start) * 1000, 3)}

    for attempt in range(50):
        try:
            health = request("/health")["body"]
            break
        except (URLError, OSError):
            if attempt == 49:
                raise
            time.sleep(0.1)
    if args.scenario == "swr" and health["mode"] != "swr":
        raise SystemExit("The SWR experiment requires CACHE_MODE=swr")
    before = request("/debug/stats")["body"]
    if before["source_reads"] or before["source_writes"]:
        raise SystemExit("Restart/recreate the API first: counters must start at zero")
    samples = []
    if args.scenario in {"sequential", "concurrent"}:
        start = time.perf_counter()
        if args.scenario == "concurrent":
            with ThreadPoolExecutor(max_workers=20) as pool:
                samples = list(pool.map(lambda _: request("/products/1"), range(20)))
        else:
            samples = [request("/products/1") for _ in range(20)]
        elapsed = round((time.perf_counter() - start) * 1000, 3)
        if elapsed >= args.ttl * 1000:
            raise SystemExit("Series exceeded the declared TTL; repeat with a larger TTL")
    elif args.scenario == "patch":
        samples.append(request("/products/1"))
        samples.append(request("/products/1", {"price_kopecks": 55000, "stock": 0}))
        samples.append(request("/products/1"))
        samples.append(request("/products/1"))
        assert samples[-1]["body"]["price_kopecks"] == 55000
        assert samples[-1]["body"]["stock"] == 0
    else:
        samples.append(request("/products/1"))
        time.sleep(args.ttl + 0.1)
        samples.append(request("/products/1"))
        if args.scenario == "swr":
            # A public-API observation; deterministic concurrency assertions are
            # in test_expert.py, using controlled source events instead of sleep.
            time.sleep(0.3)
        samples.append(request("/products/1"))
    after = request("/debug/stats")["body"]
    result = {
        "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
        "scenario": args.scenario, "health": health, "declared_fresh_ttl": args.ttl,
        "before": before, "after": after,
        "source_reads_delta": after["source_reads"] - before["source_reads"],
        "source_writes_delta": after["source_writes"] - before["source_writes"],
        "get_requests": 3 if args.scenario == "patch" else len(samples),
        "samples": samples,
    }
    if args.scenario in {"sequential", "concurrent"}:
        result["series_milliseconds"] = elapsed
    print(json.dumps(result, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
