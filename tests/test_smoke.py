import os

import httpx
import pytest

from app.main import create_app
from app.models import ProductPatch
from app.settings import Settings

pytestmark = pytest.mark.smoke


async def test_supplied_source_reads_and_updates(factory):
    lab = factory()
    before = await lab.source.get(1)
    after = await lab.source.update(1, ProductPatch(stock=7))
    assert after.stock == 7
    assert after.revision == before.revision + 1


async def test_api_contract():
    app = create_app(Settings(redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0")))
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                    base_url="http://lab") as client:
            assert (await client.get("/health")).status_code == 200
            response = await client.get("/products/1")
            assert response.status_code == 200
            assert response.json()["price_kopecks"] == 49900
            assert (await client.get("/products/999")).status_code == 404
            assert (await client.patch("/products/999", json={"stock": 1})).status_code == 404
            assert (await client.patch("/products/1", json={"stock": -1})).status_code == 422
            assert (await client.patch("/products/1", json={})).status_code == 422
            changed = await client.patch("/products/1", json={"stock": 0})
            assert changed.status_code == 200
            assert changed.json()["stock"] == 0
