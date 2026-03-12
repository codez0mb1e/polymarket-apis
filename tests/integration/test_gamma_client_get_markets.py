"""Integration tests for PolymarketGammaClient.get_markets."""

import pytest

from polymarket_apis.clients.gamma_client import PolymarketGammaClient
from polymarket_apis.types.gamma_types import GammaMarket
from tests.integration import BaseTestClient


class TestGetMarketsGammaClient(BaseTestClient[PolymarketGammaClient]):
    def _create_client(self) -> PolymarketGammaClient:
        return PolymarketGammaClient()

    def test_default_response(self) -> None:
        # Arrange / Act
        markets = self._client.get_markets(limit=self.LIMIT)

        # Assert
        assert isinstance(markets, list)
        assert len(markets) <= self.LIMIT
        assert all(isinstance(m, GammaMarket) for m in markets)
        assert all(m.id is not None for m in markets)

    @pytest.mark.parametrize(
        ("active", "closed"),
        [(True, False), (True, None), (None, True), (None, False)],
    )
    def test_status_filter(self, active: bool | None, closed: bool | None) -> None:
        # Arrange / Act
        markets = self._client.get_markets(limit=self.LIMIT, active=active, closed=closed)

        # Assert
        assert isinstance(markets, list)
        assert all(isinstance(m, GammaMarket) for m in markets)
        if active is not None:
            assert all(m.active is active for m in markets)
        if closed is not None:
            assert all(m.closed is closed for m in markets)

    @pytest.mark.parametrize("tag_id", [21, 102892])
    def test_tag_id_filter(self, tag_id: int) -> None:
        # Arrange / Act
        markets = self._client.get_markets(
            tag_id=tag_id,
            active=True,
            closed=False,
            limit=self.LIMIT,
        )

        # Assert
        assert isinstance(markets, list)
        assert all(isinstance(m, GammaMarket) for m in markets)
        assert all(m.id is not None for m in markets)
