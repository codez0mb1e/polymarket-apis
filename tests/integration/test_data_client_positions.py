"""Integration tests for PolymarketDataClient.get_positions."""

import pytest

from polymarket_apis.clients.data_client import PolymarketDataClient
from polymarket_apis.types.common import EthAddress
from polymarket_apis.types.data_types import Position
from tests.integration import BaseTestClient


class TestGetPositionsDataClient(BaseTestClient[PolymarketDataClient]):
    def _create_client(self) -> PolymarketDataClient:
        return PolymarketDataClient()

    def test_default_response(self, user: EthAddress) -> None:
        # Arrange / Act
        positions = self._client.get_positions(user=user, limit=self.LIMIT)

        # Assert
        assert isinstance(positions, list)
        assert len(positions) <= self.LIMIT
        assert all(isinstance(p, Position) for p in positions)
        assert all(p.proxy_wallet is not None for p in positions)
        assert all(p.token_id is not None for p in positions)

    def test_size_threshold_filters_small_positions(self, user: EthAddress) -> None:
        # Arrange / Act
        positions_low = self._client.get_positions(user=user, size_threshold=0.0)
        positions_high = self._client.get_positions(user=user, size_threshold=10.0)

        # Assert
        assert len(positions_low) >= len(positions_high)

    def test_limit_respected(self, user: EthAddress) -> None:
        # Arrange
        limit = 5

        # Act
        positions = self._client.get_positions(user=user, limit=limit, size_threshold=0.0)

        # Assert
        assert len(positions) <= limit

    def test_pagination_returns_different_results(self, user: EthAddress) -> None:
        # Arrange / Act
        page1 = self._client.get_positions(user=user, limit=self.LIMIT, size_threshold=0.0, offset=0)
        page2 = self._client.get_positions(user=user, limit=self.LIMIT, size_threshold=0.0, offset=self.LIMIT)

        # Assert
        ids1 = {p.token_id for p in page1}
        ids2 = {p.token_id for p in page2}
        assert ids1.isdisjoint(ids2)

    @pytest.mark.parametrize(
        ("sort_by", "sort_direction"),
        [
            ("TOKENS", "DESC"),
            ("TOKENS", "ASC"),
            ("CURRENT", "DESC"),
            ("INITIAL", "DESC"),
            ("CASHPNL", "DESC"),
            ("PERCENTPNL", "DESC"),
            ("PRICE", "DESC"),
            ("AVGPRICE", "DESC"),
        ],
    )
    def test_sorting(self, user: EthAddress, sort_by: str, sort_direction: str) -> None:
        # Arrange / Act
        positions = self._client.get_positions(
            user=user,
            limit=self.LIMIT,
            sort_by=sort_by,  # type: ignore[arg-type]
            sort_direction=sort_direction,  # type: ignore[arg-type]
        )

        # Assert
        assert isinstance(positions, list)
        assert all(isinstance(p, Position) for p in positions)

    def test_condition_id_string_filter(self, user: EthAddress) -> None:
        # Arrange: fetch all positions, then re-request by the first condition_id
        all_positions = self._client.get_positions(user=user, size_threshold=0.0)
        if not all_positions:
            pytest.skip("No positions available for this user")
        target_condition_id = all_positions[0].condition_id

        # Act
        positions = self._client.get_positions(
            user=user,
            condition_id=target_condition_id,
            size_threshold=0.0,
        )

        # Assert
        assert isinstance(positions, list)
        assert all(isinstance(p, Position) for p in positions)
        assert all(p.condition_id == target_condition_id for p in positions)

    def test_condition_id_list_filter(self, user: EthAddress) -> None:
        # Arrange: fetch multiple positions to build a list of condition_ids
        all_positions = self._client.get_positions(user=user, size_threshold=0.0, limit=5)
        if len(all_positions) < 2:
            pytest.skip("Not enough positions available for this user")
        condition_ids = list({p.condition_id for p in all_positions})[:2]

        # Act
        positions = self._client.get_positions(
            user=user,
            condition_id=condition_ids,
            size_threshold=0.0,
        )

        # Assert
        assert isinstance(positions, list)
        assert all(isinstance(p, Position) for p in positions)
        assert all(p.condition_id in condition_ids for p in positions)

    def test_redeemable_filter(self, user: EthAddress) -> None:
        # Arrange / Act
        positions = self._client.get_positions(
            user=user,
            size_threshold=0.0,
            redeemable=True,
        )

        # Assert
        assert isinstance(positions, list)
        assert all(isinstance(p, Position) for p in positions)
        assert all(p.redeemable is True for p in positions)

    def test_position_fields_are_valid(self, user: EthAddress) -> None:
        # Arrange / Act
        positions = self._client.get_positions(user=user, limit=self.LIMIT)

        # Assert
        for p in positions:
            assert p.size >= 0
            assert 0.0 <= p.avg_price <= 1.0
            assert 0.0 <= p.current_price <= 1.0
            assert p.outcome != ""
            assert p.title != ""


    def test_prod_like_request(self, user: EthAddress) -> None:
        # Arrange
        limit = 100

        # Act
        positions = self._client.get_positions(
            user=user,
            size_threshold=0.0,
            redeemable=False,
            mergeable=False,
            limit=limit,
            sort_by="TOKENS",
            sort_direction="DESC",
        )

        # Assert
        assert isinstance(positions, list)
        assert len(positions) <= limit
        assert all(isinstance(p, Position) for p in positions)
        assert all(p.size > 0 for p in positions)

