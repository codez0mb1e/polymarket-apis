"""Integration tests for PolymarketDataClient.get_accounting_snapshot_zip."""

import io
import zipfile

from polymarket_apis.clients.data_client import PolymarketDataClient
from polymarket_apis.types.common import EthAddress
from polymarket_apis.types.data_types import AccountingSnapshotCSVs
from tests.integration import BaseTestClient


class TestGetAccountingSnapshotZipDataClient(BaseTestClient[PolymarketDataClient]):
    def _create_client(self) -> PolymarketDataClient:
        return PolymarketDataClient()

    def test_returns_bytes(self, user: EthAddress) -> None:
        # Arrange / Act
        data = self._client.get_accounting_snapshot_zip(user=user)

        # Assert
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_response_is_valid_zip(self, user: EthAddress) -> None:
        # Arrange / Act
        data = self._client.get_accounting_snapshot_zip(user=user)

        # Assert
        assert zipfile.is_zipfile(io.BytesIO(data))

    def test_zip_contains_expected_csv_files(self, user: EthAddress) -> None:
        # Arrange / Act
        data = self._client.get_accounting_snapshot_zip(user=user)

        # Assert
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            names = zf.namelist()
        assert "positions.csv" in names
        assert "equity.csv" in names

    def test_csvs_are_non_empty(self, user: EthAddress) -> None:
        # Arrange / Act
        data = self._client.get_accounting_snapshot_zip(user=user)

        # Assert
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            positions_csv = zf.read("positions.csv").decode()
            equity_csv = zf.read("equity.csv").decode()

        assert len(positions_csv.strip()) > 0
        assert len(equity_csv.strip()) > 0

    def test_csvs_have_header_row(self, user: EthAddress) -> None:
        # Arrange / Act
        data = self._client.get_accounting_snapshot_zip(user=user)

        # Assert: first line must be a non-empty header
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            positions_lines = zf.read("positions.csv").decode().splitlines()
            equity_lines = zf.read("equity.csv").decode().splitlines()

        assert len(positions_lines) >= 1
        assert len(equity_lines) >= 1
        assert "," in positions_lines[0]
        assert "," in equity_lines[0]

    def test_get_accounting_snapshot_csvs_returns_model(self, user: EthAddress) -> None:
        # Arrange / Act
        result = self._client.get_accounting_snapshot_csvs(user=user)

        # Assert
        assert isinstance(result, AccountingSnapshotCSVs)
        assert isinstance(result.positions_csv, str)
        assert isinstance(result.equity_csv, str)
        assert len(result.positions_csv.strip()) > 0
        assert len(result.equity_csv.strip()) > 0
