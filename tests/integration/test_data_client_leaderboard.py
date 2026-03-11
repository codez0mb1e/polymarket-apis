"""Integration tests for PolymarketDataClient.get_leaderboard_rankings."""

import pytest

from polymarket_apis.clients.data_client import PolymarketDataClient
from polymarket_apis.types.data_types import LeaderboardUser
from tests.integration import BaseTestClient


class TestGetLeaderboardRankingsDataClient(BaseTestClient[PolymarketDataClient]):
    def _create_client(self) -> PolymarketDataClient:
        return PolymarketDataClient()


    def test_default_response(self) -> None:
        # Arrange / Act
        users = self._client.get_leaderboard_rankings(limit=self.LIMIT)

        # Assert
        assert isinstance(users, list)
        assert len(users) <= self.LIMIT
        assert all(isinstance(u, LeaderboardUser) for u in users)
        assert all(u.rank is not None for u in users)
        assert all(u.proxy_wallet is not None for u in users)
        assert [u.rank for u in users] == sorted(u.rank for u in users)

    @pytest.mark.parametrize("order_by", ["PNL", "VOL"])
    def test_order_by(self, order_by: str) -> None:
        # Arrange / Act
        users = self._client.get_leaderboard_rankings(limit=self.LIMIT, order_by=order_by)  # type: ignore[arg-type]

        # Assert
        assert isinstance(users, list)
        assert all(isinstance(u, LeaderboardUser) for u in users)
        assert all(u.rank is not None for u in users)

    @pytest.mark.parametrize("time_period", ["DAY", "WEEK", "MONTH", "ALL"])
    def test_time_period(self, time_period: str) -> None:
        # Arrange / Act
        users = self._client.get_leaderboard_rankings(limit=self.LIMIT, time_period=time_period)  # type: ignore[arg-type]

        # Assert
        assert isinstance(users, list)
        assert all(isinstance(u, LeaderboardUser) for u in users)
        assert all(u.rank is not None for u in users)

    @pytest.mark.parametrize(
        "category",
        ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "CULTURE", "ECONOMICS"],
    )
    def test_category_filter(self, category: str) -> None:
        # Arrange / Act
        users = self._client.get_leaderboard_rankings(limit=self.LIMIT, category=category)  # type: ignore[arg-type]

        # Assert
        assert isinstance(users, list)
        assert all(isinstance(u, LeaderboardUser) for u in users)
        assert all(u.rank is not None for u in users)

    def test_pagination_offset_returns_different_results(self) -> None:
        # Arrange / Act
        page1 = self._client.get_leaderboard_rankings(limit=self.LIMIT, offset=0)
        page2 = self._client.get_leaderboard_rankings(limit=self.LIMIT, offset=self.LIMIT)

        # Assert
        wallets1 = {u.proxy_wallet for u in page1}
        wallets2 = {u.proxy_wallet for u in page2}
        assert wallets1.isdisjoint(wallets2)
