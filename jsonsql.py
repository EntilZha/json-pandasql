import json
import pandas as pd
from pandasql import PandaSQL


class MaxBy:
    def __init__(self):
        self._max = None
        self._obj = None

    def step(self, key, obj):
        max_ = json.loads(obj)[key]
        if self._max is None or max_ > self._max:
            self._max = max_
            self._obj = obj

    def finalize(self):
        return self._obj


class MinBy:
    def __init__(self):
        self._min = None
        self._obj = None

    def step(self, key, obj):
        min_ = json.loads(obj)[key]
        if self._min is None or min < self._min:
            self._min = min_
            self._obj = obj

    def finalize(self):
        return self._obj


class JsonPandaSql(PandaSQL):
    def _init_connection(self, conn):
        conn.connection.connection.create_aggregate("maxby", 2, MaxBy)
        conn.connection.connection.create_aggregate("minby", 2, MinBy)
