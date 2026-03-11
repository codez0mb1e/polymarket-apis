"""Integration tests for PolymarketGammaClient.get_events."""

import datetime

import pytest

from polymarket_apis.clients.gamma_client import PolymarketGammaClient
from polymarket_apis.types.gamma_types import Event
from tests.integration import BaseTestClient


class TestGetEventsGammaClient(BaseTestClient[PolymarketGammaClient]):
    def _create_client(self) -> PolymarketGammaClient:
        return PolymarketGammaClient()

    def test_default_response(self) -> None:
        # Arrange / Act
        events = self._client.get_events(limit=self.LIMIT)

        # Assert
        assert isinstance(events, list)
        assert len(events) <= self.LIMIT
        assert all(isinstance(e, Event) for e in events)
        assert all(e.id is not None for e in events)

    @pytest.mark.parametrize(("active", "closed"), [(True, None), (None, True)])
    def test_status_filter(self, active: bool | None, closed: bool | None) -> None:
        # Arrange / Act
        events = self._client.get_events(limit=self.LIMIT, active=active, closed=closed)

        # Assert
        if active is not None:
            assert all(e.active is True for e in events)
        if closed is not None:
            assert all(e.closed is True for e in events)

    @pytest.mark.parametrize("ascending", [True, False])
    def test_ordering_by_id(self, ascending: bool) -> None:
        # Arrange / Act
        events = self._client.get_events(
            limit=self.LIMIT, order="id", ascending=ascending
        )

        # Assert
        ids = [e.id for e in events]
        assert ids == sorted(ids, reverse=not ascending)

    def test_offset_returns_different_results(self) -> None:
        # Arrange / Act
        page1 = self._client.get_events(
            limit=self.LIMIT, offset=0, order="id", ascending=True
        )
        page2 = self._client.get_events(
            limit=self.LIMIT, offset=self.LIMIT, order="id", ascending=True
        )

        # Assert
        ids1 = {e.id for e in page1}
        ids2 = {e.id for e in page2}
        assert ids1.isdisjoint(ids2)

    @pytest.mark.parametrize(
        ("volume_min", "liquidity_min"),
        [
            (1_000.0, None),
            (None, 500.0),
        ],
    )
    def test_numeric_min_filter(
        self, volume_min: float | None, liquidity_min: float | None
    ) -> None:
        # Arrange / Act
        events = self._client.get_events(
            limit=self.LIMIT, volume_min=volume_min, liquidity_min=liquidity_min
        )

        # Assert
        if volume_min is not None:
            assert all(e.volume is None or e.volume >= volume_min for e in events)
        if liquidity_min is not None:
            assert all(
                e.liquidity is None or e.liquidity >= liquidity_min for e in events
            )

    def test_end_date_min_filter(self) -> None:
        # Arrange
        cutoff = datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC)

        # Act
        events = self._client.get_events(limit=self.LIMIT, end_date_min=cutoff)

        # Assert
        assert all(e.end_date is None or e.end_date >= cutoff for e in events)

    @pytest.mark.parametrize("tag_slug", ["crypto", "politics"])
    def test_filter_by_tag_slug(self, tag_slug: str) -> None:
        # Arrange / Act
        events = self._client.get_events(limit=self.LIMIT, tag_slug=tag_slug)

        # Assert
        assert isinstance(events, list)
        assert all(isinstance(e, Event) for e in events)
        assert all(e.id is not None for e in events)
