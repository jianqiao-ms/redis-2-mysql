#! /usr/bin/python
#-* coding: utf-8 -*

import os
import json

CONFIG_FILE = os.path.join(os.path.dirname(__file__), 'config.json')

class NewDict(dict):
    def __getattr__(self, item):
        return self.get(item)
    def __setattr__(self, key, value):
        self.__setitem__(key, value)

class JSONConfig(NewDict):
    def __init__(self, _):
        super(JSONConfig, self).__init__()
        self.update(_)


with open(CONFIG_FILE, 'r') as file:
    config = json.load(file, object_hook = JSONConfig)

