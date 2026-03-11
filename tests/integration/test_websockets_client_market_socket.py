"""Integration tests for PolymarketWebsocketsClient.market_socket."""

import contextlib
import threading

from lomond.events import Text

from polymarket_apis.clients.websockets_client import (
    PolymarketWebsocketsClient,
    parse_market_event,
)
from polymarket_apis.types.websockets_types import (
    BestBidAskEvent,
    LastTradePriceEvent,
    MarketResolvedEvent,
    NewMarketEvent,
    OrderBookSummaryEvent,
    PriceChangeEvent,
    TickSizeChangeEvent,
)
from tests.integration import BaseTestClient


class TestMarketSocketWebsocketsClient(BaseTestClient[PolymarketWebsocketsClient]):
    def _create_client(self) -> PolymarketWebsocketsClient:
        return PolymarketWebsocketsClient()

    def test_market_socket_receives_events(self) -> None:
        """Test that market_socket successfully connects and receives valid market events."""
        # Arrange
        token_ids = [
            "28432382570210071531339763350179732116550480614535496455646131884743577260983",
            "18694016142208574015034281753037516184884334091522694908800738541271770986351",
        ]
        received_events: list[
            OrderBookSummaryEvent
            | PriceChangeEvent
            | TickSizeChangeEvent
            | LastTradePriceEvent
            | BestBidAskEvent
            | NewMarketEvent
            | MarketResolvedEvent
            | list[OrderBookSummaryEvent]
        ] = []
        max_events = 5
        timeout_seconds = 30

        def process_event(text: Text) -> None:
            """Collect parsed events and stop after receiving enough."""
            event = parse_market_event(text)
            if event is not None:
                received_events.append(event)
                if len(received_events) >= max_events:
                    # Signal to stop the websocket loop
                    raise KeyboardInterrupt

        def run_socket() -> None:
            """Run the market socket in a separate thread."""
            with contextlib.suppress(KeyboardInterrupt):
                self._client.market_socket(
                    token_ids=token_ids,
                    custom_feature_enabled=True,
                    process_event=process_event,
                )

        # Act
        socket_thread = threading.Thread(target=run_socket, daemon=True)
        socket_thread.start()
        socket_thread.join(timeout=timeout_seconds)

        # Assert
        assert len(received_events) > 0, "Should receive at least one event"

        # Verify all received events are valid MarketEvents
        for event in received_events:
            if isinstance(event, list):
                # OrderBookSummaryEvent can come as a list
                assert all(isinstance(e, OrderBookSummaryEvent) for e in event)
                # Verify token_id is present and valid
                assert all(e.token_id for e in event)
            else:
                # Verify it's one of the expected event types
                assert isinstance(
                    event,
                    (
                        OrderBookSummaryEvent,
                        PriceChangeEvent,
                        TickSizeChangeEvent,
                        LastTradePriceEvent,
                        BestBidAskEvent,
                        NewMarketEvent,
                        MarketResolvedEvent,
                    ),
                )

    def test_market_socket_with_custom_features_disabled(self) -> None:
        """Test market_socket with custom_feature_enabled=False."""
        # Arrange
        token_ids = [
            "28432382570210071531339763350179732116550480614535496455646131884743577260983",
            "18694016142208574015034281753037516184884334091522694908800738541271770986351",
        ]
        received_events: list[
            OrderBookSummaryEvent
            | PriceChangeEvent
            | TickSizeChangeEvent
            | LastTradePriceEvent
            | BestBidAskEvent
            | NewMarketEvent
            | MarketResolvedEvent
            | list[OrderBookSummaryEvent]
        ] = []
        max_events = 3
        timeout_seconds = 30

        def process_event(text: Text) -> None:
            """Collect parsed events and stop after receiving enough."""
            event = parse_market_event(text)
            if event is not None:
                received_events.append(event)
                if len(received_events) >= max_events:
                    raise KeyboardInterrupt

        def run_socket() -> None:
            """Run the market socket in a separate thread."""
            with contextlib.suppress(KeyboardInterrupt):
                self._client.market_socket(
                    token_ids=token_ids,
                    custom_feature_enabled=False,
                    process_event=process_event,
                )

        # Act
        socket_thread = threading.Thread(target=run_socket, daemon=True)
        socket_thread.start()
        socket_thread.join(timeout=timeout_seconds)

        # Assert
        assert len(received_events) > 0, "Should receive at least one event"

        for event in received_events:
            if isinstance(event, list):
                assert all(isinstance(e, OrderBookSummaryEvent) for e in event)
            else:
                assert isinstance(
                    event,
                    (
                        OrderBookSummaryEvent,
                        PriceChangeEvent,
                        TickSizeChangeEvent,
                        LastTradePriceEvent,
                        BestBidAskEvent,
                        NewMarketEvent,
                        MarketResolvedEvent,
                    ),
                )
