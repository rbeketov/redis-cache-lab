import argparse
import asyncio
import json
import time

import httpx


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrent", action="store_true")
    args = parser.parse_args()
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000", timeout=10) as client:
        async def get(path):
            response = await client.get(path)
            response.raise_for_status()
            return response.json()

        for attempt in range(20):
            try:
                health = await get("/health")
                break
            except httpx.ConnectError:
                if attempt == 19:
                    raise
                await asyncio.sleep(0.25)
        before = await get("/debug/stats")
        start = time.perf_counter()
        if args.concurrent:
            products = await asyncio.gather(*(get("/products/1") for _ in range(20)))
        else:
            products = [await get("/products/1") for _ in range(20)]
        elapsed = time.perf_counter() - start
        after = await get("/debug/stats")
        assert all(product == products[0] for product in products)
        print(json.dumps({
            "scenario": "concurrent" if args.concurrent else "sequential",
            "requests": len(products), "mode": health["mode"],
            "before": before, "after": after,
            "source_reads_delta": after["source_reads"] - before["source_reads"],
            "elapsed_seconds": round(elapsed, 3),
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
