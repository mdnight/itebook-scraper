import asyncio
import click
import logging
from functools import partial

from os import environ as _environ
from tornado.ioloop import IOLoop
from tornado.httpclient import AsyncHTTPClient
from tornado.queues import Queue
from typing import List, Dict

from helpers import fetch_book, ScrapingType
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


async def parse_and_write(scraping_type: ScrapingType):
    http_client = AsyncHTTPClient()
    response = await http_client.fetch(TARGET_URL)
    response_body = response.body.decode()
    if scraping_type == ScrapingType.books:
        category_links = Scraper.get_categories_urls(response_body)
        book_links = []
        for category_url in category_links:
            links = await Scraper.get_book_page_links(http_client, category_url)
            book_links = [*book_links, *links]
        await fetch_book_info_iteratively(book_links)
    elif scraping_type == ScrapingType.categories:
        categories = Scraper.get_categories(response_body)
        await send_categories_to_queue(categories)


async def fetch_book_info_iteratively(book_links: List[str], n=50):
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
    await q.put(END_MESSAGE)


async def send_categories_to_queue(categories):
    for category in categories:
        await q.put(category)
        await asyncio.sleep(0.5)
    await q.put(END_MESSAGE)


async def save_to_db(collection_name: str = None):
    db = connect_to_db(DB_HOST, DB_PORT, DB_NAME, MONGO_INITDB_ROOT_USERNAME, MONGO_INITDB_ROOT_PASSWORD)
    collection_name = collection_name if collection_name else DB_COLLECTION_NAME
    collection = set_collection(db, collection_name)
    async for item in q:
        if item == END_MESSAGE:
            q.task_done()
            return
        try:
            await insert_to_db(collection, item)
        finally:
            q.task_done()


@click.command()
@click.option('--type', prompt='What to scrape: books or categories', default='books')
@click.option('--collection', prompt='Name of collection.', default=None)
def main(type, collection):
    loop = IOLoop.instance()
    scraping_type = ScrapingType[type]
    if type and scraping_type == ScrapingType.categories:
        collection = collection
    else:
        collection = None
    asyncio.ensure_future(save_to_db(collection))
    loop.run_sync(partial(parse_and_write, scraping_type))


if __name__ == '__main__':
    main()
