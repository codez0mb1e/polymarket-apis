"""
Order Book Composer.

The key points:
- For given pair asset ids subscribe to market_socket() of WS client
- Listen OrderBookSummaryEvent and visualize it as Order Book (use rich for visualization in terminal)
- Listen new orders via web-socket and actualize Order Book.
"""

# %% Imports ----
import signal
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

import polars as pl
from lomond.events import Text
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from polymarket_apis.clients.websockets_client import (
    PolymarketWebsocketsClient,
    parse_market_event,
)
from polymarket_apis.types.clob_types import OrderSummary
from polymarket_apis.types.websockets_types import (
    LastTradePriceEvent,
    OrderBookSummaryEvent,
    PriceChangeEvent,
)

# %% Constants ----

# https://gamma-api.polymarket.com/markets?slug=btc-updown-5m-1773232200

marker_slug = "ethereum-up-or-down-march-124"

token_ids: Final[dict[str, str]] = {
    "115296421197388697879349230796240062735526030776147866945358625922780240459674": "Token UP",
    "40003453655508075727983149701309988407868499288804390240611905294885634486371": "Token DOWN",
}


SAVE_SNAPSHOT: Final[bool] = True
SNAPSHOT_DIR: Final[Path] = Path("data/bitcoin_up_or_down_5min")
MAX_LEVELS: Final[int] = 5  # price levels shown per side

# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------


@dataclass
class BookState:
    """Mutable order book state for a single token."""

    token_id: str
    bids: dict[float, float] = field(default_factory=dict[float, float])  # price -> size
    asks: dict[float, float] = field(default_factory=dict[float, float])  # price -> size
    last_trade_price: float | None = None
    last_trade_size: float | None = None
    tx_hash: str | None = None
    initialized: bool = False


def _make_state() -> dict[str, BookState]:
    return {tid: BookState(token_id=tid) for tid, _ in token_ids.items()}


order_books: dict[str, BookState] = _make_state()

# ---------------------------------------------------------------------------
# Snapshot accumulation buffers (populated when SAVE_SNAPSHOT is True)
# ---------------------------------------------------------------------------

bbo_records: list[dict[str, Any]] = []    # top-N bid/ask levels per book update
trade_records: list[dict[str, Any]] = []  # last-trade-price events

# ---------------------------------------------------------------------------
# State update helpers
# ---------------------------------------------------------------------------


def _record_book_levels(server_ts: datetime, local_ts: datetime, state: BookState) -> None:
    """Append top-N bid and ask levels from *state* to bbo_records."""
    top_bids = sorted(state.bids.items(), key=lambda x: x[0], reverse=True)[:MAX_LEVELS]
    top_asks = sorted(state.asks.items(), key=lambda x: x[0])[:MAX_LEVELS]
    for level, (price, size) in enumerate(top_bids, 1):
        bbo_records.append(
            {"server_timestamp": server_ts, "local_timestamp": local_ts, "token_id": state.token_id, "side": "BID", "level": level, "price": price, "size": size}
        )
    for level, (price, size) in enumerate(top_asks, 1):
        bbo_records.append(
            {"server_timestamp": server_ts, "local_timestamp": local_ts, "token_id": state.token_id, "side": "ASK", "level": level, "price": price, "size": size}
        )


def _apply_snapshot(state: BookState, event: OrderBookSummaryEvent) -> None:
    """Replace the full book with a snapshot from an OrderBookSummaryEvent."""
    state.bids = {o.price: o.size for o in event.bids}
    state.asks = {o.price: o.size for o in event.asks}
    if event.last_trade_price is not None:
        state.last_trade_price = event.last_trade_price
    state.initialized = True


def _apply_price_change(state: BookState, price: float, size: float, side: str) -> None:
    """Apply a single price-level update from a PriceChangeEvent."""
    book_side = state.bids if side == "BUY" else state.asks
    if size == 0:
        book_side.pop(price, None)
    else:
        book_side[price] = size


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def _book_table(state: BookState) -> Table:
    short_id = token_ids.get(state.token_id, state.token_id[:6])  # friendly name or truncated id

    table = Table(
        title=f"[bold]{short_id}[/bold]",
        box=box.SIMPLE_HEAD,
        show_header=True,
        header_style="bold white",
        min_width=38,
    )
    table.add_column("Price, ¢", justify="right", width=10)
    table.add_column("Shares", justify="right", width=12)

    if not state.initialized:
        table.add_row("[dim]waiting…[/dim]", "")
        return table

    asks: list[OrderSummary] = sorted(
        (OrderSummary(price=p, size=s) for p, s in state.asks.items()),
        key=lambda o: o.price,
    )[:MAX_LEVELS]
    bids: list[OrderSummary] = sorted(
        (OrderSummary(price=p, size=s) for p, s in state.bids.items()),
        key=lambda o: o.price,
        reverse=True,
    )[:MAX_LEVELS]

    # Asks displayed top-to-bottom, highest ask first (farthest from mid at top)
    for ask in reversed(asks):
        table.add_row(
            f"[red]{ask.price * 100:.2f}[/red]",
            f"[red]{ask.size:.2f}[/red]",
        )

    table.add_section()
    if asks and bids:
        spread = (asks[0].price - bids[0].price) * 100
        table.add_row(
            f"[bold yellow]{spread:.2f}[/bold yellow]",
            "[dim]spread[/dim]",
        )
    else:
        table.add_row("[dim]—[/dim]", "[dim]spread[/dim]")
    table.add_section()

    # Bids displayed top-to-bottom, best bid first (closest to mid at top)
    for bid in bids:
        table.add_row(
            f"[green]{bid.price * 100:.2f}[/green]",
            f"[green]{bid.size:.2f}[/green]",
        )

    if state.last_trade_price is not None:
        table.add_section()
        price_str = f"{state.last_trade_price * 100:.2f}"
        size_str = f"{state.last_trade_size:.2f}" if state.last_trade_size is not None else "—"
        table.add_row(
            f"[dim]Last trade {price_str}\u00a2  x{size_str}[/dim]\u00a2 [dim](tx: {state.tx_hash[:6] if state.tx_hash else '—'})[/dim]",
            "",
        )

    return table


def _render() -> Panel:
    tables = [_book_table(order_books[tid]) for tid in token_ids]
    return Panel(
        Columns(tables, equal=True, expand=True),
        title="[bold cyan]Bitcoin Up or Down - 5 Minutes[/bold cyan]",
        border_style="bright_blue",
    )


# ---------------------------------------------------------------------------
# Event processing
# ---------------------------------------------------------------------------


def _process_event(text: Text) -> None:
    local_ts = datetime.now(tz=UTC)
    ev = parse_market_event(text)

    if isinstance(ev, list):
        # Initial multi-token snapshot delivered as a list
        for snapshot in ev:
            state = order_books.get(snapshot.token_id)
            if state is not None:
                _apply_snapshot(state, snapshot)

    elif isinstance(ev, OrderBookSummaryEvent):
        state = order_books.get(ev.token_id)
        if state is not None:
            _apply_snapshot(state, ev)

    elif isinstance(ev, PriceChangeEvent):
        affected: set[str] = set()
        for pc in ev.price_changes:
            state = order_books.get(pc.token_id)
            if state is None or not state.initialized:
                continue
            _apply_price_change(state, pc.price, pc.size, pc.side)
            affected.add(pc.token_id)
        if SAVE_SNAPSHOT:
            for token_id in affected:
                _record_book_levels(ev.timestamp, local_ts, order_books[token_id])

    elif isinstance(ev, LastTradePriceEvent):
        state = order_books.get(ev.token_id)
        if state is not None:
            state.last_trade_price = ev.price
            state.last_trade_size = ev.size
            state.tx_hash = ev.transaction_hash  # type: ignore[assignment]  # add transaction hash to state for demonstration
        if SAVE_SNAPSHOT:
            trade_records.append({
                "server_timestamp": ev.timestamp,
                "local_timestamp": local_ts,
                "token_id": ev.token_id,
                "price": ev.price,
                "size": ev.size,
                "side": ev.side,
                "tx_hash": ev.transaction_hash,
            })

# %% Subscribe to Polymarket Live Data Websockets ----
# Using live_data_socket() of PolymarketWebsocketsClient to subscribe to market price for the specified token ids.
# subscriptions = list(
#     list(
#       topic = "crypto_prices_chainlink",
#       type = "*",
#       filters = '{"symbol":"btc/usd"}'
#     )
#   )
# Add a callback function to process incoming price events and print the price to the console.

# %% Entry point ----


def _save_snapshots() -> None:
    """Write accumulated BBO and trade records to parquet files."""
    if not SAVE_SNAPSHOT:
        return

    market_time = marker_slug.rsplit("-", maxsplit=1)[-1]
    bbo_path = SNAPSHOT_DIR / f"ob_snapshots_{market_time}.parquet"
    pl.DataFrame(bbo_records).write_parquet(bbo_path)

    trades_path = SNAPSHOT_DIR / f"trades_{market_time}.parquet"
    pl.DataFrame(trade_records).write_parquet(trades_path)

    print(f"Snapshots saved → {bbo_path}  |  {trades_path}")


def main() -> None:
    console = Console()
    client = PolymarketWebsocketsClient()

    def _shutdown(signum: int, frame: object) -> None:  # noqa: ARG001
        _save_snapshots()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        with Live(_render(), console=console, refresh_per_second=10, screen=True) as live:

            def on_event(text: Text) -> None:
                _process_event(text)
                live.update(_render())

            client.market_socket(token_ids=list(token_ids), process_event=on_event)
    finally:
        _save_snapshots()


if __name__ == "__main__":
    main()
