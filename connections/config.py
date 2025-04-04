import json
import logging
import os
from collections import defaultdict

logger = logging.getLogger()

class Config:
    def __init__(self):
        self.mongodb_connection_string = None

    def reload_from_secrets_file(self):
        dictionary = defaultdict(lambda: "")

        try:
            secrets_file_path = os.environ["SECRETS_FILE"]
            dictionary = json.loads(open(secrets_file_path).read())
        except (FileNotFoundError, Exception):
            logger.info("FAILED to load Secrets file")

        self.mongodb_connection_string = dictionary.get("MONGODB_CONNECTION_STRING")