from typing import TYPE_CHECKING
from motor.motor_asyncio import AsyncIOMotorClient

if TYPE_CHECKING:
    from motor.motor_asyncio import AsyncIOMotorDatabase, AsyncIOMotorCollection


def connect_to_db(host: str, port: int, dbname: str, username: str, password: str) -> 'AsyncIOMotorDatabase':
    url = f"mongodb://{username}:{password}@{host}:{port}"
    client = AsyncIOMotorClient(url)
    return client[dbname]


def set_collection(db: 'AsyncIOMotorDatabase', collection: str) -> 'AsyncIOMotorCollection':
    return db[collection]


async def insert_to_db(db_collection: 'AsyncIOMotorCollection', data: dict) -> str:
    await db_collection.insert_one(data)
