import asyncio
import logging

from os import environ as _environ
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado.queues import Queue
from typing import List

from helpers import fetch_book
from scraper import Scraper
from storage import connect_to_db, insert_to_db, set_collection

DB_HOST = _environ.get('DB_HOSTNAME', 'localhost')
DB_PORT = int(_environ.get('DB_PORT', 27017))
DB_NAME = _environ.get('DB_NAME')
DB_COLLECTION_NAME = _environ.get('DB_COLLECTION_NAME')
MONGO_INITDB_ROOT_USERNAME = _environ.get('MONGO_INITDB_ROOT_USERNAME')
MONGO_INITDB_ROOT_PASSWORD = _environ.get('MONGO_INITDB_ROOT_PASSWORD')
TARGET_URL = _environ.get('TARGET_URL')

LOGGING = logging.getLogger(__name__)

q = Queue()
END_MESSAGE = '_END_'


async def parse_and_write():
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(TARGET_URL)
    category_links = Scraper.get_categories_urls(response.body.decode())
    book_links = []
    for category_url in category_links:
        links = await Scraper.get_book_page_links(http_client, category_url)
        book_links = [*book_links, *links]
    await fetch_book_info_iteratively(book_links)


async def fetch_book_info_iteratively(book_links: List[str], n=20):
    fetched, total = 0, len(book_links)
    book_links = [book_links[i:i + n] for i in range(len(book_links) // n)]
    for chunk in book_links:
        futures = []
        for book_item_url in chunk:
            futures.append(fetch_book(book_item_url))
        for future in asyncio.as_completed(futures):
            page = await future
            fetched += 1
            print(f'\rFetched book info: {fetched}/{total}', end='')
            book_data = Scraper.parse_book_info(page)
            if book_data:
                await q.put(book_data)
    q.put(END_MESSAGE)


async def save_to_db():
    db = connect_to_db(DB_HOST, DB_PORT, DB_NAME, MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD)
    collection = set_collection(db, DB_COLLECTION_NAME)
    async for book in q:
        if book == END_MESSAGE:
            q.task_done()
            return
        try:
            await insert_to_db(collection, book)
        finally:
            q.task_done()


def main():
    loop = IOLoop.instance()
    asyncio.ensure_future(save_to_db())
    loop.run_sync(parse_and_write)


if __name__ == '__main__':
    main()
