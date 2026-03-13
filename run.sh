#!/usr/bin/env bash

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] running examples/order_book_composer.py"
  uv run examples/order_book_composer.py
done
