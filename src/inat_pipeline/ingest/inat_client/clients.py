from typing import Iterator

from .base import BaseInatClient, _chunked
from .config import EndpointConfig

"""
# Init params
self.params = config.params

# Add params_key with temp value to params
self.params[config.param_key] = None

# Add 'per_page' field to params
if config.per_page is not None:
    self.params["per_page"] = config.per_page

# Add provided fields to params
if self.fields is not None:
    self.params["fields"] = self.fields
"""


class BatchEndpointClient(BaseInatClient):
    """
    For endpoints like GET /observations/
    - Sends chunk_size IDs as a comma-joined 'id' param
    - Supports sparse fieldsets via ?fields=
    - Supports per_page pagination
    """

    def __init__(self, config: EndpointConfig, fetcher, writer):
        assert config.id_param, "ParametrizedEndpointClient requires config.id_param"
        super().__init__(config, fetcher, writer)

    def _iter_requests(self, ids: list) -> Iterator[dict]:
        for chunk in _chunked(ids, self.config.chunk_size):
            params = {
                **self.config.params,
                "id": ",".join(map(str, chunk)),
            }
            if self.config.fields:
                params["fields"] = ",".join(self.config.fields)
            yield params


class ParametrizedEndpointClient(BaseInatClient):
    """
    For endpoints like GET /observations?taxon_id=1234
    - One ID per request (chunk_size is ignored / forced to 1)
    - Named param specified by config.id_param
    - Still paginates — can return many results per ID
    """

    def __init__(self, config: EndpointConfig, fetcher, writer):
        assert config.id_param, "ParametrizedEndpointClient requires config.id_param"
        super().__init__(config, fetcher, writer)

    def _iter_requests(self, ids: list) -> Iterator[dict]:
        for id_ in ids:  # always 1 ID per request
            yield {
                **self.config.params,
                self.config.id_param: id_,
            }
