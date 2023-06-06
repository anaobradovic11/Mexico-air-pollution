import logging
from typing import Any

from dotenv import dotenv_values
from pymongo import MongoClient

config = dotenv_values(".env")

logger = logging.getLogger(__name__)


class MongoDBClient:
    def __init__(self) -> None:
        self.client: MongoClient = MongoClient(config["SERVER_URI"])

    def init_database(self) -> None:
        self.database = self.client.get_database(config["DB_NAME"])
        logger.info(f"Connection to database {config['DB_NAME']} initialized.")

    def insert(self, data: list[dict[str, Any]], table_name: str) -> None:
        collection = self.database.get_collection(table_name)

        logger.info(f"Inserting data to table {table_name}.")
        collection.insert_many(data)
        logger.info(f"Batch of length: {len(data)} inserted.")
