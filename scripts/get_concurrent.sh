#!/bin/sh
docker compose exec -T api python - <<'PY'
import asyncio
import httpx

async def main():
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000") as client:
        before = (await client.get("/debug/stats")).json()["source_reads"]
        responses = await asyncio.gather(
            *(client.get("/products/2") for _ in range(20)))
        for response in responses:
            response.raise_for_status()
        after = (await client.get("/debug/stats")).json()["source_reads"]
        print("Успешных GET:", len(responses))
        print("Чтений источника:", after - before)

asyncio.run(main())
PY