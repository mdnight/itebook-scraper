import logging
from enum import Enum
from tornado.httpclient import AsyncHTTPClient
from typing import Optional, Tuple


class ScrapingType(Enum):
    books = 1
    categories = 2


async def fetch_book(url: str) -> Optional[Tuple[str, str]]:
    http_client = AsyncHTTPClient()
    try:
        response = await http_client.fetch(url)
    except Exception as e:
        logging.error(f'Failed to fetch page of book, {e}')
        return
    logging.info(f'Fetched: {url}')
    return response.body.decode()

