import asyncio
import logging
from typing import Dict, List

import aiohttp
from tqdm.asyncio import tqdm_asyncio

from ..ingest.inat_client import BinaryFetcher, LocalBinaryWriter

logger = logging.getLogger(__name__)


async def _download_photo(
    session: aiohttp.ClientSession,
    fetcher: BinaryFetcher,
    writer: LocalBinaryWriter,
    photo_id: str,
    attribute_id: str,
    extension: str,
    size: str,
):
    """Orchestrate the download and write of a single photo."""
    url = f"https://inaturalist-open-data.s3.amazonaws.com/photos/{photo_id}/{size}.{extension}"
    filename = f"{photo_id}.{extension}"

    try:
        data = await fetcher.fetch(session, url)
        await writer.write(data, attribute_id, filename)
    except Exception as e:
        logger.error("Failed to download photo %s: %s", photo_id, e)


async def execute_async(
    items: List[Dict],
    parent_folder: str,
    extension: str,
    size: str,
    rate: int,
):
    """Async execution of the photo download batch."""
    fetcher = BinaryFetcher(rate=rate)
    writer = LocalBinaryWriter(parent_folder)

    async with aiohttp.ClientSession() as session:
        tasks = [
            _download_photo(
                session,
                fetcher,
                writer,
                str(item["photo_id"]),
                str(item["attribute_id"]),
                extension,
                size,
            )
            for item in items
        ]
        await tqdm_asyncio.gather(*tasks)

    writer.close()


def execute(
    items: List[Dict],
    parent_folder: str,
    extension: str = "jpg",
    size: str = "medium",
    rate: int = 60,
) -> None:
    """
    Workflow entry point to download a list of photos.

    Args:
        items: List of dicts with 'photo_id' and 'attribute_id'.
        parent_folder: Path to save the photos.
        extension: Image extension (e.g. 'jpg').
        size: iNat image size (e.g. 'medium', 'large', 'original').
        rate: Requests per minute limit.
    """
    logger.info("Starting photo download for %d items", len(items))
    asyncio.run(execute_async(items, parent_folder, extension, size, rate))
    logger.info("Photo download complete.")
