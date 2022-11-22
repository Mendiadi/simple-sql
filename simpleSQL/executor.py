from __future__ import annotations
from ctypes import Union
from typing import Callable, Any

import mysql.connector
import enum


class DBTable:

    def __getattribute__(self, item):
        return super(DBTable, self).__getattribute__(item)

    def __getattr__(self, item):
        return super(DBTable, self).__setattr__(item)

    def __str__(self):
        return f"TABLE = {str(self.__dict__)}"

    def __repr__(self):
        return str(self)

    def __setitem__(self, key, value):
        setattr(self, key, value)


class SQLCommand(enum.Enum):
    select = "SELECT"
    distinct = "DISTINCT"
    order = "ORDER BY"
    insert = "INSERT"
    into = "INTO"
    where = "WHERE"


class SQLExecutor:
    def __init__(self, *args, **kwargs):
        self.db = mysql.connector.connect(*args, **kwargs)
        self._cursor = self.db.cursor()
        self._buffer = None

    def __enter__(self):
        return SimpleSQL(self)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._cursor.close()
        self.db.close()

    def _packing_query(self):
        res = []

        for cols in self._cursor:
            t = DBTable()
            for i, col in enumerate(self._cursor.column_names):
                t[col] = cols[i]
            res.append(t)
        return res

    def execute_select(self, table,
                       columns: Union[tuple[str], list[str], str] = "*",
                       sorted: str = None,
                       distinct: bool = False,
                       condition: str = ""):
        if columns != "*": columns = ", ".join(columns)[1:]
        if condition: condition = f"{SQLCommand.where.value} {condition}"
        if not sorted: sorted = ""


        self._cursor.execute(f"{SQLCommand.select.value} {columns} FROM {table} {sorted} {condition};")
        return self._packing_query()


class SimpleSQL:
    def __init__(self, executor: SQLExecutor = None):
        self._executor = executor

    @staticmethod
    def connect(*args, **kwargs) -> SQLExecutor:
        return SQLExecutor(*args, **kwargs)


    def query_filters(self,table:type,filters:str,first:bool=False):
        result = self._executor.execute_select(table.__name__, condition= filters)
        return [table(**item.__dict__) for item in result] if not first else table(**result[0].__dict__)

    def query_filter_by(self, table: type, filter: str, filter_value: Any, first=False):
        result = self._executor.execute_select(table.__name__, condition=f"{filter} = \"{filter_value}\"")
        return [table(**item.__dict__) for item in result] if not first else table(**result[0].__dict__)

    def query_all(self, table: type):
        result = self._executor.execute_select(table.__name__)
        return [table(**item.__dict__) for item in result]


    def query_ordered(self, table: type,key:str,reverse:bool = False):
        if key:
            key = f"{SQLCommand.order.value} {key}"
        result = self._executor.execute_select(table.__name__,sorted=key)
        return [table(**item.__dict__) for item in result]