from .clients import BatchEndpointClient, ParametrizedEndpointClient
from .config import EndpointConfig
from .protocols import Fetcher, Writer


def make_client(config: EndpointConfig, fetcher: Fetcher, writer: Writer):
    """Pick the right client based on whether an id_param is set."""
    if config.id_param:
        return ParametrizedEndpointClient(config, fetcher, writer)
    return BatchEndpointClient(config, fetcher, writer)
