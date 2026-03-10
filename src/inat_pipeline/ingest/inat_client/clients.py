import logging
from typing import Iterator

from .base import BaseInatClient, _chunked
from .config import EndpointConfig

logger = logging.getLogger(__name__)


class BatchEndpointClient(BaseInatClient):
    """
    For endpoints like GET /observations/
    - Sends chunk_size IDs as a comma-joined 'id' param
    - Supports sparse fieldsets via ?fields=
    - Supports per_page pagination
    """

    def _iter_requests(self, ids: list) -> Iterator[dict]:
        for chunk in _chunked(ids, self.config.chunk_size):
            params = {
                **self.config.params,
            }
            if self.config.fields:
                params["fields"] = self.config.fields

            # Modify url to append listed ids
            self.config.url + ",".join(map(str, chunk))
            logger.debug(self.config.url)
            yield params


class ParametrizedEndpointClient(BaseInatClient):
    """
    For endpoints like GET /observations?taxon_id=1234
    - One ID per request (chunk_size is ignored / forced to 1)
    - Named param specified by config.id_param
    - Still paginates — can return many results per ID
    """

    def __init__(self, config: EndpointConfig, fetcher, writer):
        assert (
            config.id_param is not None
        ), "ParametrizedEndpointClient requires config.id_param"
        super().__init__(config, fetcher, writer)

    def _iter_requests(self, ids: list) -> Iterator[dict]:
        for id_ in ids:  # always 1 ID per request
            yield {
                **self.config.params,
                self.config.id_param: id_,
            }
