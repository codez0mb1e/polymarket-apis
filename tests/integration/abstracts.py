"""Shared base classes for integration tests."""

from abc import ABC, abstractmethod
from typing import Final, TypeVar

import pytest

from polymarket_apis.types.common import EthAddress

TClient = TypeVar("TClient")


class BaseTestClient[TClient](ABC):
    """Base class for integration tests of Polymarket API clients."""

    LIMIT: Final[int] = 10

    @abstractmethod
    def _create_client(self) -> TClient: ...

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        self._client: TClient = self._create_client()

    @pytest.fixture(params=["0xd84c2b6d65dc596f49c7b6aadd6d74ca91e407b9"])
    def user(self, request: pytest.FixtureRequest) -> EthAddress:
        return request.param  # type: ignore[no-any-return]
