#! -*- coding: utf-8 -*-
import os
import yaml

PROJECT_NAME = "TAP API"

PROJECT_PATH = os.path.dirname(__file__)
CONFIG_FILE = os.path.join(PROJECT_PATH, "settings.yaml")
COOKIE_SECRET = "11a0f5a5df09021b09ae9811ee0c2c11e64c781b"

MOOC_GROUP_KEY = 2
DISPATCH_OPTIMIZE = True

with open(CONFIG_FILE) as f:
    document = f.read()
    locals().update(yaml.load(document))

