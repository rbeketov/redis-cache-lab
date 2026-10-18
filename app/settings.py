import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    redis_url: str = "redis://localhost:6379/0"
    fresh_ttl: float = 5.0
    stale_ttl: float = 10.0
    source_delay: float = 0.1
    mode: str = "cache_aside"

    def __post_init__(self):
        if self.mode not in {"cache_aside", "swr"}:
            raise ValueError("CACHE_MODE must be cache_aside or swr")
        if self.fresh_ttl <= 0 or self.stale_ttl < 0 or self.source_delay < 0:
            raise ValueError("Invalid TTL or source delay")

    @classmethod
    def from_env(cls):
        return cls(
            redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            fresh_ttl=float(os.getenv("FRESH_TTL", "5")),
            stale_ttl=float(os.getenv("STALE_TTL", "10")),
            source_delay=float(os.getenv("SOURCE_DELAY", "0.1")),
            mode=os.getenv("CACHE_MODE", "cache_aside"),
        )
