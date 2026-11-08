#!/bin/bash

curl -fsS http://localhost:8000/debug/stats; echo
for i in {1..20}; do
  curl -fsS http://localhost:8000/products/1 > /dev/null || break
done
curl -fsS http://localhost:8000/debug/stats; echo