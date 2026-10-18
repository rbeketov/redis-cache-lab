"""Supplied primary data source. Its delay models an expensive external read."""
import asyncio

from app.models import Product, ProductPatch


class CatalogSource:
    def __init__(self, delay: float = 0.1):
        self.delay = delay
        self.reads = 0
        self.writes = 0
        self.products = {
            1: Product(id=1, title="Кофе 250 г", price_kopecks=49900, stock=20, revision=1),
            2: Product(id=2, title="Чай 100 г", price_kopecks=29900, stock=30, revision=1),
        }

    async def get(self, product_id: int) -> Product:
        self.reads += 1
        # Snapshot before the delay makes stale-fill races reproducible.
        product = self.products.get(product_id)
        await asyncio.sleep(self.delay)
        if product is None:
            raise KeyError(product_id)
        return product.model_copy(deep=True)

    async def update(self, product_id: int, patch: ProductPatch) -> Product:
        product = self.products[product_id]
        updated = product.model_copy(update={
            **patch.model_dump(exclude_none=True), "revision": product.revision + 1,
        })
        self.products[product_id] = updated
        self.writes += 1
        return updated.model_copy(deep=True)
