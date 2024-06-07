import logging
import json


class CustomConfigParser:

    def __init__(self, session, file_path):
        self.session = session
        try:
            _config_object = session.read.json(file_path).collect()[0][0]
            self.config_object = json.loads(_config_object)
        except IndexError:
            logging.error("File not found in the following path:"
                          + str(file_path))
            exit()

    def __getitem__(self, key):
        return self.config_object[key]
