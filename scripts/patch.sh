#!/bin/bash
curl -fsS http://localhost:8000/products/1; echo
curl -fsS -X PATCH http://localhost:8000/products/1 \
  -H 'Content-Type: application/json' \
  -d '{"price_kopecks":55000,"stock":0}'; echo
curl -fsS http://localhost:8000/products/1; echo
curl -fsS http://localhost:8000/debug/stats; echo