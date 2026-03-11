"""Integration tests for PolymarketDataClient.get_activity."""

import datetime

import pytest

from polymarket_apis.clients.data_client import PolymarketDataClient
from polymarket_apis.types.common import EthAddress
from polymarket_apis.types.data_types import Activity
from tests.integration import BaseTestClient


class TestGetActivityDataClient(BaseTestClient[PolymarketDataClient]):
    def _create_client(self) -> PolymarketDataClient:
        return PolymarketDataClient()


    def test_default_response(self, user: EthAddress) -> None:
        # Arrange / Act
        activities = self._client.get_activity(user=user, limit=self.LIMIT)

        # Assert
        assert isinstance(activities, list)
        assert len(activities) <= self.LIMIT
        assert all(isinstance(a, Activity) for a in activities)
        assert all(a.proxy_wallet is not None for a in activities)
        assert all(a.transaction_hash is not None for a in activities)

    @pytest.mark.parametrize(
        ("sort_by", "sort_direction"),
        [
            ("TIMESTAMP", "DESC"),
            ("TIMESTAMP", "ASC"),
            ("TOKENS", "DESC"),
            ("CASH", "DESC"),
        ],
    )
    def test_sorting(self, user: EthAddress, sort_by: str, sort_direction: str) -> None:
        # Arrange / Act
        activities = self._client.get_activity(
            user=user,
            limit=self.LIMIT,
            sort_by=sort_by,  # type: ignore[arg-type]
            sort_direction=sort_direction,  # type: ignore[arg-type]
        )

        # Assert
        assert isinstance(activities, list)
        assert all(isinstance(a, Activity) for a in activities)

    @pytest.mark.parametrize(
        "activity_type",
        ["TRADE", "SPLIT", "MERGE", "REDEEM", "REWARD", "CONVERSION", "MAKER_REBATE", "YIELD"],
    )
    def test_type_filter(self, user: EthAddress, activity_type: str) -> None:
        # Arrange / Act
        activities = self._client.get_activity(
            user=user,
            limit=self.LIMIT,
            type=activity_type,  # type: ignore[arg-type]
        )

        # Assert
        assert isinstance(activities, list)
        assert all(isinstance(a, Activity) for a in activities)
        assert all(a.type == activity_type for a in activities)

    @pytest.mark.parametrize("side", ["BUY", "SELL"])
    def test_side_filter(self, user: EthAddress, side: str) -> None:
        # Arrange / Act
        activities = self._client.get_activity(
            user=user,
            limit=self.LIMIT,
            side=side,  # type: ignore[arg-type]
        )

        # Assert
        assert isinstance(activities, list)
        assert all(isinstance(a, Activity) for a in activities)
        assert all(a.side == side for a in activities)

    def test_date_range_filter(self, user: EthAddress) -> None:
        # Arrange
        start = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
        end = datetime.datetime(2026, 3, 1, tzinfo=datetime.UTC)

        # Act
        activities = self._client.get_activity(
            user=user, limit=self.LIMIT, start=start, end=end
        )

        # Assert
        assert isinstance(activities, list)
        assert all(isinstance(a, Activity) for a in activities)
        assert all(start <= a.timestamp.replace(tzinfo=datetime.UTC) <= end for a in activities)

    def test_pagination_offset_returns_results(self, user: EthAddress) -> None:
        # Arrange / Act
        page2 = self._client.get_activity(user=user, limit=self.LIMIT, offset=self.LIMIT)

        # Assert
        assert isinstance(page2, list)
        assert len(page2) <= self.LIMIT
        assert all(isinstance(a, Activity) for a in page2)

    def test_prod_like_request(self, user: EthAddress) -> None:
        # Arrange
        end = datetime.datetime.now(tz=datetime.UTC)
        start = end - datetime.timedelta(days=30)
        limit = 1000

        # Act
        activities = self._client.get_activity(
            user=user,
            limit=limit,
            start=start,
            end=end,
            sort_by="TIMESTAMP",  # type: ignore[arg-type]
            sort_direction="DESC",  # type: ignore[arg-type]
        )

        # Assert
        assert isinstance(activities, list)
        assert all(isinstance(a, Activity) for a in activities)
