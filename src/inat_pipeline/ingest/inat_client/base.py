import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Queue
from itertools import islice
from typing import Iterator

import aiohttp
from tqdm.asyncio import tqdm_asyncio

from ...utils.git import get_git_hash
from .config import EndpointConfig
from .protocols import Fetcher, Writer

logger = logging.getLogger(__name__)


def _chunked(iterable, n):
    it = iter(iterable)
    while chunk := list(islice(it, n)):
        yield chunk


class BaseInatClient(ABC):
    """
    Shared execution loop: iterate requests → paginate each → write results.
    Subclasses only implement _iter_requests() to control how IDs map to HTTP params.
    """

    def __init__(self, config: EndpointConfig, fetcher: Fetcher, writer: Writer):
        # Copy config to client
        self.config = config

        self.fetcher = fetcher
        self.writer = writer
        # Init writer
        self.queue = Queue()

        # Get git has for version
        self.version = get_git_hash(short=True)

    @abstractmethod
    def _iter_requests(self, ids: list) -> Iterator[dict]:
        """Yield one param-dict per HTTP request (before pagination)."""
        ...

    async def execute(
        self,
        ids: list[str],
    ):
        # Store count of observers (for logger)
        async with aiohttp.ClientSession() as session:
            writer_task = asyncio.create_task(self._consume_queue())

            # Create fetchers with batched IDs
            fetch_tasks = [
                asyncio.create_task(self._fetch_all_pages(session, base_params))
                for base_params in self._iter_requests(ids)
            ]

            await tqdm_asyncio.gather(*fetch_tasks)  # all producers done
            await self.queue.join()  # queue drained
            await self.queue.put(None)  # sentinel → stop consumer
            await writer_task

        if hasattr(self.writer, "close"):  # graceful executor shutdown
            self.writer.close()

    async def _fetch_all_pages(
        self, session: aiohttp.ClientSession, base_params: dict
    ) -> None:
        """Producer: fetch all pages for one logical request, enqueue each page."""
        page = 1
        while True:
            params = {**base_params, "page": page, "per_page": self.config.per_page}
            response = await self.fetcher.fetch(session, self.config.url, params)
            results = response.get("results", [])

            if not results:
                break

            await self.queue.put(results)  # backpressure if consumer is slow

            total = response.get("total_results", 0)
            if page * self.config.per_page >= total:
                break

            page += 1

    async def _consume_queue(self) -> None:
        """Consumer: drain the queue and delegate to writer."""
        processed = 0

        while True:
            page = await self.queue.get()  # blocks until a page arrives
            if page is None:  # sentinel
                self.queue.task_done()
                logger.info("Writer done, processed %d items", processed)
                break

            # Optional: greedily drain any immediately available pages into one batch
            batch = page
            while not self.queue.empty():
                next_page = self.queue.get_nowait()
                if next_page is None:
                    await self.queue.put(None)  # put sentinel back, handle next loop
                    break
                batch = batch + next_page
                self.queue.task_done()

            await self.writer.write(batch)
            processed += len(batch)
            self.queue.task_done()
