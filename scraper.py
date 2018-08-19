import asyncio
import logging

from bs4 import BeautifulSoup
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    from tornado.httpclient import AsyncHTTPClient

LOGGING = logging.getLogger(__name__)


class Scraper:
    @staticmethod
    def get_categories_urls(html_doc: str) -> List[str]:
        soup = BeautifulSoup(html_doc, 'html.parser')
        list_items = soup.body.select('#menu-categories')[0].find_all('li')
        return [item.select('a')[0].get('href') for item in list_items]

    @classmethod
    async def get_book_page_links(cls, http_client: 'AsyncHTTPClient', category_url: str) -> List[str]:
        response = await http_client.fetch(category_url)
        soup = BeautifulSoup(response.body.decode(), 'html.parser')
        pagination_range = soup.body.select('.pagination')[0].find_all('a')[-1].text
        futures = []
        for page_num in range(1, int(pagination_range) + 1):
            futures.append(http_client.fetch(f'{category_url}page/{page_num}/'))
        result = []
        for future in asyncio.as_completed(futures):
            try:
                response = await future
            except Exception as e:
                LOGGING.error(f'Raised error during page request: {e}')
                continue
            urls = cls.parse_books_urls(response.body.decode())
            result = [*result, *urls]

        return result

    @staticmethod
    def parse_books_urls(html_doc: str) -> List[str]:
        soup = BeautifulSoup(html_doc, 'html.parser')
        items = soup.body.select('.post')
        result = [item.a.attrs['href'] for item in items]
        [print(f'\r{item}', end='') for item in result]
        return result

    @staticmethod
    def parse_book_info(html_doc: str):
        bs = BeautifulSoup(html_doc, 'html.parser')
        try:
            book_info = dict(zip([item.text for item in bs.body.dl.find_all('dt')],
                                 [item.text for item in bs.body.dl.find_all('dd')]))
        except Exception as e:
            LOGGING.error(f'Book info parsing error: {e}')
            return
        return {
            'single_title': bs.body.find(class_='single-title').text,
            'author': book_info.get('Author:', '').strip(),
            'isbn_10': book_info.get('ISBN-10:', '').strip(),
            'year': int(book_info.get('Year:', '0').strip()),
            'pages': int(book_info.get('Pages:', '0').strip()),
            'language': book_info['Language:'].strip(),
            'size': book_info.get('File size:', '').strip(),
            'format': book_info.get('File format:', '').strip(),
            'category': book_info.get('Category:', '').strip(),
            'book_urls': [item.attrs.get('href') for item in bs.find(class_='download-links').find_all('a')],
            'title_photo': bs.body.img.attrs.get('src'),
            'description': bs.body.find(class_='entry-content').text,
        }
