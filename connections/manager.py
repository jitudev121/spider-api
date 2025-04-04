import sys

from loguru import logger
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from pymongo import AsyncMongoClient

from connections.config import Config

class Manager:
    def __init__(self):
        self.config = Config()

        for handler in list(logger._core.handlers):
            logger.remove(handler)
        logger.add(sys.stdout, level="INFO", format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS zz}</green> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}):</yellow> {message}", colorize=True)

        self.config.reload_from_secrets_file()


        connection = MongoClient(
            self.config.mongodb_connection_string,
            server_api=ServerApi("1"),
        )

        connection_async = AsyncMongoClient(
            self.config.mongodb_connection_string,
            server_api=ServerApi("1"),
        )

        self.db = connection["influencer_db"]
        self.db_async = connection_async["influencer_db"]

    def reestablish_connections(self):
        self.config.reload_from_secrets_file()
