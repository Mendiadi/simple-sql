from __future__ import annotations
from ctypes import Union
from typing import Callable, Any

import mysql.connector
import enum


class DatabaseNotExist(Exception):
    def __init__(self, msg):
        super().__init__("Database not exists. " + msg)


class DatabaseExist(Exception):
    def __init__(self, msg):
        super().__init__("Cant create database that already exists. " + msg)


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
        self.db = None
        self._cursor = None
        self._buffer = None
        if kwargs.get("create_and_ignore", None):

            self._auto_create_and_ignore(*args, **kwargs)
        else:
            self._connection(*args, **kwargs)

    def __enter__(self):
        return SimpleSQL(self)

    def _auto_create_and_ignore(self, *args, **kwargs):
        name = kwargs.get("database", None)
        if name:
            kwargs["database"] = None
        self._connection(*args, **kwargs)
        if name not in [dbs[0] for dbs in self.databases()]:
            self.execute_create_db(name)
            self.db.database = name
        else:
            self.db.database = name

    def _connection(self, *args, **kwargs):
        kwargs.pop("create_and_ignore",None)
        self.db = mysql.connector.connect(*args, **kwargs)
        self._cursor = self.db.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):

        self._cursor.close()
        self.db.close()

    def databases(self) -> list:
        self.execute("show databases")
        return self._cursor.fetchall()

    def _adding_quot(self, values):
        values_ = []
        for val in values:
            values_.append("\"" + str(val) + "\"")
        return values_

    def _packing_query(self):
        res = []
        for cols in self._cursor:
            t = DBTable()
            for i, col in enumerate(self._cursor.column_names):
                t[col] = cols[i]
            res.append(t)
        return res

    def commit(self):
        self.db.commit()

    def execute_select(self, table,
                       columns: Union[tuple[str], list[str], str] = "*",
                       sorted: str = None,
                       distinct: bool = False,
                       condition: str = ""):
        if columns != "*": columns = ", ".join(columns)[1:]
        if condition: condition = f"{SQLCommand.where.value} {condition}"
        if not sorted: sorted = ""
        self.execute(f"{SQLCommand.select.value} {columns} FROM {table} {sorted} {condition};")
        return self._packing_query()

    def execute_create_db(self, name: str):
        self.execute(f"CREATE DATABASE {name};")
        self.db.database = name

    def execute_drop_db(self, name: str):
        self.execute(f"DROP DATABASE {name};")

    def execute_insert(self, table, columns: tuple, values: tuple):
        values = self._adding_quot(values)
        cols = f'({str(",").join(columns)})'
        vals = f'({str(",").join(values)})'
        self.execute(f"{SQLCommand.insert.value} {SQLCommand.into.value} {table}"
                     f" {cols} VALUES {vals};")

    def start(self):
        return self.__enter__()

    def execute_create_table(self, name: str, columns: tuple, primary):
        if not primary:
            primary = ""
        else:
            primary = f", PRIMARY KEY ({primary}) "
        print(f"CREATE TABLE {name} ({str(',').join(columns)}{primary});")
        self.execute(f"CREATE TABLE {name} ({str(',').join(columns)}{primary});")

    def stop(self):
        self.__exit__(None, None, None)

    def execute_delete_by(self, table, column, value):
        self.execute(f"DELETE FROM {table} WHERE {column} = \"{value}\";")

    def execute_delete_if_equal(self, table, statement):
        self.execute(f"DELETE FROM {table} WHERE {statement};")

    def execute_drop_table(self, table: str):
        self.execute(f"DROP TABLE IF EXISTS {table}")

    def execute_increment_value(self, val: int):
        self.execute(f"ALTER TABLE Persons AUTO_INCREMENT={val};")

    def execute_backup(self, database: str, filepath: str, diff: bool = False):
        if diff:
            diff_ = " WITH DIFFERENTIAL"
        else:
            diff_ = ""
        self.execute(f"BACKUP DATABASE {database} TO DISK = '{filepath}'{diff_};")

    def execute_update_table(self, table, data, condition=None, filters: list[tuple] = None):
        if filters:
            filters_parse = [f"{c} = \'{v}\'" for c, v in filters if data.__dict__[c] != v]
        else:
            filters_parse = [f"{c} = \'{v}\'" for c, v in data.__dict__.items()]

        if condition:
            condition = f" WHERE {condition}"
        else:
            condition = f" WHERE {filters_parse.pop(0)}"

        self.execute(f"UPDATE {table} SET {','.join(filters_parse)}{condition};")

    def execute(self, statement):
        self._cursor.execute(statement)


class SQLTypes:
    @staticmethod
    def integer():
        return f"int"

    @staticmethod
    def varchar(size: int, ):
        return f"varchar({size})"

    @staticmethod
    def column(d_type, nullable: bool = True, auto_increment: bool = False):
        if nullable:
            nullable = ""
        else:
            nullable = " NOT NULL"
        if auto_increment:
            auto_increment = " AUTO_INCREMENT"

        else:
            auto_increment = ""
        print(f"{d_type}{nullable}{auto_increment}")
        return f"{d_type}{nullable}{auto_increment}"


class SimpleSQL:
    def __init__(self, executor: SQLExecutor = None):
        self._executor = executor
        self._types = SQLTypes()

    @property
    def executor(self) -> SQLExecutor:
        return self._executor

    @property
    def types(self) -> SQLTypes:
        return self._types

    def set_auto_commit(self, val: bool):
        self._executor.db.autocommit = val

    def drop_database(self, name):
        if name not in self.local_databases():
            raise DatabaseNotExist(f"you cant drop none exists database named \"{name}\"")
        self._executor.execute_drop_db(name)

    def create_database(self, name):
        if name not in self.local_databases():
            self._executor.execute_create_db(name)
        else:
            raise DatabaseExist(f"database named \"{name}\" is already created.")

    def create_table(self, table: type, data, primary_key: str = None, auto_increment_value: int = None):
        self._executor.execute_create_table(table.__name__ if not isinstance(table, str) else table,
                                            tuple([f"{obj} {type_}" for obj, type_ in data.__dict__.items()]),
                                            primary_key)
        if auto_increment_value:
            print(auto_increment_value)
            self._executor.execute_increment_value(auto_increment_value)

    def query_filters(self, table: type, filters: str, first: bool = False):
        result = self._executor.execute_select(table.__name__, condition=filters)
        return [table(**item.__dict__) for item in result] if not first else table(**result[0].__dict__)

    def query_filter_by(self, table: type, filter: str, filter_value: Any, first=False):
        result = self._executor.execute_select(table.__name__, condition=f"{filter} = \"{filter_value}\"")
        return [table(**item.__dict__) for item in result] if not first else table(**result[0].__dict__)

    def query_all(self, table: type):
        result = self._executor.execute_select(table.__name__)
        return [table(**item.__dict__) for item in result]

    def insert_to(self, table: type, data):
        self._executor.execute_insert(table.__name__, tuple(data.__dict__.keys()), tuple(data.__dict__.values()))

    def query_ordered(self, table: type, key: str, reverse: bool = False):
        if key:
            key = f"{SQLCommand.order.value} {key}"
        result = self._executor.execute_select(table.__name__, sorted=key)
        return [table(**item.__dict__) for item in result]

    def query_delete_by(self, table: type, filter_by: tuple[str, Any]):
        self._executor.execute_delete_by(table.__name__, filter_by[0], filter_by[1])

    def drop_table(self, table: Union[str, type]):
        self._executor.execute_drop_table(table.__name__ if not isinstance(table, str) else table)

    def local_databases(self) -> list:
        return [db[0] for db in self._executor.databases()]

    def query_update_table(self, table, data):
        self._executor.execute_update_table(table.__name__,data)

    def query_alter_table(self):
        ...

    def backup(self, filepath: str, diff: bool = False):
        if self._executor.db.database:
            self._executor.execute_backup(self._executor.db.database, filepath, diff)
        else:
            raise DatabaseNotExist(f"connected with {self._executor.db.database} database."
                                   f"consider to connect or created database to backup, or just use executor.execute_backup()")

    def add(self, instance: Any):
        try:
            temp = 0
            for a,v in instance.__dict__.items():
                if v == "primary_key":
                    instance.__dict__[a] = 0
                    temp = a
            self.insert_to(type(instance), instance)

        except mysql.connector.errors.ProgrammingError as e:
            if e.errno != 1146:
                raise e
            instance.__dict__[temp] = "primary_key"
            temp = type(instance)(**instance.__dict__)
            print(temp)
            self.create_table(*self._prepare_table(instance))
            self.insert_to(type(temp), temp)

    def delete(self, instance: Any):
        r = ""
        for k, v in instance.__dict__.items():
            r += f" {k} = \"{v}\" AND"
        self._executor.execute_delete_if_equal(type(instance).__name__, r[:-3:])

    def _prepare_table(self, instance):
        table_name = type(instance).__name__
        temp = {}
        primary = inc = None
        for attribute, value in instance.__dict__.items():
            if value == "primary_key":
                primary,inc = attribute, 1
            if isinstance(value, int):
                temp[attribute] = self._types.column(self._types.integer(),auto_increment=True)
            elif isinstance(value, str):
                temp[attribute] = self._types.column(self._types.varchar(50))
            else:
                temp[attribute] = None
            print(attribute,value)
        instance.__dict__ = temp
        return table_name, instance, primary, inc


def connect(*args, **kwargs) -> SQLExecutor:
    return SQLExecutor(*args, **kwargs)
