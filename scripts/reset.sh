#!/bin/bash

CACHE_MODE=cache_aside FRESH_TTL=60 SOURCE_DELAY=0.1 \
  docker compose up -d --force-recreate api
curl --fail --retry 10 --retry-connrefused --retry-delay 1 \
  http://localhost:8000/health
