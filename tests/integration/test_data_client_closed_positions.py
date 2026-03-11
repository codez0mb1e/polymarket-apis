"""Integration tests for PolymarketDataClient.get_closed_positions."""

import pytest

from polymarket_apis.clients.data_client import PolymarketDataClient
from polymarket_apis.types.common import EthAddress
from polymarket_apis.types.data_types import ClosedPosition
from tests.integration import BaseTestClient


class TestGetClosedPositionsDataClient(BaseTestClient[PolymarketDataClient]):
    def _create_client(self) -> PolymarketDataClient:
        return PolymarketDataClient()

    def test_default_response(self, user: EthAddress) -> None:
        # Arrange / Act
        positions = self._client.get_closed_positions(user=user)

        # Assert
        assert isinstance(positions, list)
        assert len(positions) > 0
        assert all(isinstance(p, ClosedPosition) for p in positions)

    def test_fields_are_populated(self, user: EthAddress) -> None:
        # Arrange / Act
        positions = self._client.get_closed_positions(user=user)

        if not positions:
            pytest.skip("No closed positions available for this user")

        # Assert
        for p in positions:
            assert p.proxy_wallet is not None
            assert p.token_id != ""
            assert p.condition_id != ""
            assert p.outcome != ""
            assert p.title != ""
            assert p.slug != ""
            assert p.end_date is not None

    def test_condition_id_string_filter(self, user: EthAddress) -> None:
        # Arrange: fetch all closed positions, then re-request by the first condition_id
        all_positions = self._client.get_closed_positions(user=user)
        if not all_positions:
            pytest.skip("No closed positions available for this user")
        target_condition_id = all_positions[0].condition_id

        # Act
        positions = self._client.get_closed_positions(
            user=user,
            condition_ids=target_condition_id,
        )

        # Assert
        assert isinstance(positions, list)
        assert len(positions) > 0
        assert all(isinstance(p, ClosedPosition) for p in positions)
        assert all(p.condition_id == target_condition_id for p in positions)

    def test_condition_id_list_filter(self, user: EthAddress) -> None:
        # Arrange: fetch all closed positions, take first two distinct condition_ids
        all_positions = self._client.get_closed_positions(user=user)
        condition_ids = list({p.condition_id for p in all_positions})[:2]
        if len(condition_ids) < 2:
            pytest.skip("Not enough distinct condition_ids for this user")

        # Act
        positions = self._client.get_closed_positions(
            user=user,
            condition_ids=condition_ids,
        )

        # Assert
        assert isinstance(positions, list)
        assert len(positions) > 0
        assert all(isinstance(p, ClosedPosition) for p in positions)
        returned_ids = {p.condition_id for p in positions}
        assert returned_ids.issubset(set(condition_ids))

    def test_proxy_wallet_matches_user(self, user: EthAddress) -> None:
        # Arrange / Act
        positions = self._client.get_closed_positions(user=user)

        if not positions:
            pytest.skip("No closed positions available for this user")

        # Assert: proxy_wallet should correspond to the requested user
        assert all(p.proxy_wallet.lower() == user.lower() for p in positions)

    def test_financial_fields_are_numeric(self, user: EthAddress) -> None:
        # Arrange / Act
        positions = self._client.get_closed_positions(user=user)

        if not positions:
            pytest.skip("No closed positions available for this user")

        # Assert
        for p in positions:
            assert isinstance(p.avg_price, float)
            assert isinstance(p.current_price, float)
            assert isinstance(p.total_bought, float)
            assert isinstance(p.realized_pnl, float)
            assert 0.0 <= p.avg_price <= 1.0
            assert 0.0 <= p.current_price <= 1.0
            assert p.total_bought >= 0.0

    def test_prod_like_response(self, user: EthAddress) -> None:
        # Arrange / Act
        positions = self._client.get_closed_positions(user=user)

        if not positions:
            pytest.skip("No closed positions available for this user")

        # Assert: check for fields that are commonly populated in production data
        assert any(p.realized_pnl != 0.0 for p in positions)
        assert any(p.total_bought > 0.0 for p in positions)
