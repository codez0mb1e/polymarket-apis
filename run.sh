#!/usr/bin/env bash

SYMBOL="${1:-btc}"

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] running examples/order_book_composer.py --symbol ${SYMBOL}"
  uv run examples/order_book_composer.py --symbol "${SYMBOL}"
done
