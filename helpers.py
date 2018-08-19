import logging
from tornado.httpclient import AsyncHTTPClient
from typing import Optional, Tuple


async def fetch_book(url: str) -> Optional[Tuple[str, str]]:
    http_client = AsyncHTTPClient()
    try:
        response = await http_client.fetch(url)
    except Exception as e:
        logging.error(f'Failed to fetch page of book, {e}')
        return
    logging.info(f'Fetched: {url}')
    return response.body.decode()

