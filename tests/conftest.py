# encoding=utf-8

import os
import sys

sys.path.insert(1, os.path.join(os.path.dirname(__file__), ".."))


pytest_plugins = ["pytest_asyncio"]  # pylint: disable=invalid-name
