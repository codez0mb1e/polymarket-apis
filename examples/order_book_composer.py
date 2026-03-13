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
import threading
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

from polymarket_apis.clients.gamma_client import PolymarketGammaClient
from polymarket_apis.clients.websockets_client import (
    PolymarketWebsocketsClient,
    parse_live_data_event,
    parse_market_event,
)
from polymarket_apis.types.clob_types import OrderSummary
from polymarket_apis.types.websockets_types import (
    AssetPriceUpdateEvent,
    LastTradePriceEvent,
    OrderBookSummaryEvent,
    PriceChangeEvent,
)

# %% Constants ----
SAVE_SNAPSHOT: Final[bool] = True
SNAPSHOT_DIR: Final[Path] = Path("data/btc-updown-5m")
MAX_LEVELS: Final[int] = 5  # price levels shown per side
PRICE_FEED_SYMBOL: Final[str] = "btc/usd"

TARGET_TAG_ID: Final[int] = 102892  # "5 Minutes" tag_id on Polymarket

# %% Market and token selection ----
gamma_client = PolymarketGammaClient()
markets = gamma_client.get_markets(active=True, closed=False, tag_id=TARGET_TAG_ID, limit=100)

markets = [m for m in markets if m.slug.startswith("btc-updown-5m")]
target_time = int(datetime.now(tz=UTC).timestamp())  # target market expiring ~5 minutes from now (allowing some buffer for clock skew and market selection)
target_market = min(markets, key=lambda m: abs(int(m.slug.rsplit("-", maxsplit=1)[-1]) - target_time))


market_slug = target_market.slug
market_time = int(target_market.slug.rsplit("-", maxsplit=1)[-1])
token_ids: dict[str, str] = {
    target_market.token_ids[0]: "UP",
    target_market.token_ids[1]: "DOWN",
}

# %% State ----
@dataclass
class BookState:
    """Mutable order book state for a single token."""

    token_id: str
    bids: dict[float, float] = field(
        default_factory=dict[float, float]
    )  # price -> size
    asks: dict[float, float] = field(
        default_factory=dict[float, float]
    )  # price -> size
    last_trade_price: float | None = None
    last_trade_size: float | None = None
    tx_hash: str | None = None
    initialized: bool = False


def _make_state() -> dict[str, BookState]:
    return {tid: BookState(token_id=tid) for tid, _ in token_ids.items()}


order_books: dict[str, BookState] = _make_state()


# %% Snapshot accumulation buffers (populated when SAVE_SNAPSHOT is True) ----
bbo_records: list[dict[str, Any]] = []  # top-N bid/ask levels per book update
trade_records: list[dict[str, Any]] = []  # last-trade-price events
price_feed_records: list[dict[str, Any]] = []  # price feed events
current_price: float | None = None


# %% State update helpers ----
def _record_book_levels(
    server_ts: datetime, local_ts: datetime, state: BookState
) -> None:
    """Append top-N bid and ask levels from *state* to bbo_records."""
    top_bids = sorted(state.bids.items(), key=lambda x: x[0], reverse=True)[:MAX_LEVELS]
    top_asks = sorted(state.asks.items(), key=lambda x: x[0])[:MAX_LEVELS]
    for level, (price, size) in enumerate(top_bids, 1):
        bbo_records.append(
            {
                "server_timestamp": server_ts,
                "local_timestamp": local_ts,
                "token_id": state.token_id,
                "side": "BID",
                "level": level,
                "price": price,
                "size": size,
            }
        )
    for level, (price, size) in enumerate(top_asks, 1):
        bbo_records.append(
            {
                "server_timestamp": server_ts,
                "local_timestamp": local_ts,
                "token_id": state.token_id,
                "side": "ASK",
                "level": level,
                "price": price,
                "size": size,
            }
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


# %% Rendering ----
def _book_table(state: BookState) -> Table:
    short_id = token_ids.get(
        state.token_id, state.token_id[:6]
    )  # friendly name or truncated id

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
        size_str = (
            f"{state.last_trade_size:.2f}" if state.last_trade_size is not None else "—"
        )
        table.add_row(
            f"[dim]Last trade {price_str}\u00a2  x{size_str}[/dim]\u00a2 [dim](tx: {state.tx_hash[:6] if state.tx_hash else '—'})[/dim]",
            "",
        )

    return table


def _render() -> Panel:
    tables = [_book_table(order_books[tid]) for tid in token_ids]
    price_str = (
        f"  [white]{PRICE_FEED_SYMBOL.upper()} ${current_price:.2f}[/white]"
        if current_price is not None
        else ""
    )
    return Panel(
        Columns(tables, equal=True, expand=True),
        title=f"[bold cyan]{market_slug}[/bold cyan] {price_str}",
        border_style="bright_blue",
    )


# %% Event processing ----
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
            trade_records.append(
                {
                    "server_timestamp": ev.timestamp,
                    "local_timestamp": local_ts,
                    "token_id": ev.token_id,
                    "price": ev.price,
                    "size": ev.size,
                    "side": ev.side,
                    "tx_hash": ev.transaction_hash,
                }
            )


# %% Subscribe to Polymarket Live Data Websockets ----
price_feed_subscriptions: list[dict[str, Any]] = [
    {
        "topic": "crypto_prices_chainlink",
        "type": "*",
        "filters": f'{{"symbol":"{PRICE_FEED_SYMBOL}"}}',
    }
]


def _on_price_event(text: Text) -> None:
    """Callback for live_data_socket: updates current price and optionally records it."""
    global current_price  # noqa: PLW0603
    local_ts = datetime.now(tz=UTC)
    ev = parse_live_data_event(text)
    if isinstance(ev, AssetPriceUpdateEvent):
        current_price = float(ev.payload.value)
        if SAVE_SNAPSHOT:
            price_feed_records.append(
                {
                    "server_timestamp": ev.timestamp,
                    "local_timestamp": local_ts,
                    "price": current_price,
                }
            )


# %% Entry point ----
def _save_snapshots(market_time: int) -> None:
    """Write accumulated BBO and trade records to parquet files."""
    if not SAVE_SNAPSHOT:
        return

    bbo_path = SNAPSHOT_DIR / f"ob_snapshots_{market_time}.parquet"
    pl.DataFrame(bbo_records).write_parquet(bbo_path)

    trades_path = SNAPSHOT_DIR / f"trades_{market_time}.parquet"
    pl.DataFrame(trade_records).write_parquet(trades_path)

    price_feed_path = SNAPSHOT_DIR / f"prices_{market_time}.parquet"
    pl.DataFrame(price_feed_records).write_parquet(price_feed_path)

    print(f"Snapshots saved → {bbo_path}  |  {trades_path}  |  {price_feed_path}")


def main() -> None:
    console = Console()
    client = PolymarketWebsocketsClient()

    def _shutdown(signum: int, frame: object) -> None:  # noqa: ARG001
        _save_snapshots(market_time)
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        with Live(
            _render(), console=console, refresh_per_second=10, screen=True
        ) as live:

            def on_market_event(text: Text) -> None:
                _process_event(text)
                live.update(_render())

            def on_price_event(text: Text) -> None:
                _on_price_event(text)
                live.update(_render())

            threading.Thread(
                target=client.live_data_socket,
                kwargs={
                    "subscriptions": price_feed_subscriptions,
                    "process_event": on_price_event,
                },
                daemon=True,
            ).start()

            client.market_socket(
                token_ids=list(token_ids), process_event=on_market_event
            )
    finally:
        _save_snapshots(market_time)


if __name__ == "__main__":
    main()
