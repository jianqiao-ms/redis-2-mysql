#! /usr/bin/python
#-* coding: utf-8 -*

import os, io
import json

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')

class NewDict(dict):
    def __getattr__(self, item):
        return self.get(item)
    def __setattr__(self, key, value):
        self.__setitem__(key, value)

class Config(NewDict):
    def save(self):
        with io.open(CONFIG_FILE, 'w', encoding='utf8') as config_file:
            config_file.write(unicode(json.dumps(self, ensure_ascii=False, indent=2)))

with open(CONFIG_FILE, 'r') as file:
    config = json.load(file, object_pairs_hook = Config)

