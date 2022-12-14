from __future__ import annotations

import asyncio
import json
import os
from ctypes import Union
from typing import Any, Sequence, Iterable

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
        self._is_conn = False
        self.db = None
        self._cursor = None
        self._buffer = None

    def __enter__(self):
        return SimpleSQL(self)

    @staticmethod
    def _adding_quot(values, columns):
        values_ = []
        new_cols =[]
        for i, val in enumerate(values):
            if list == type(val):
                val = json.dumps({"list": val})
            if dict == type(val):
                val = json.dumps({"dict": val})
            if val is None:
                values_.append("null")
            elif val == "AUTO_INC_VALUE":

                continue

            else:
                values_.append("\'" + str(val) + "\'")
            new_cols.append(columns[i])

        return new_cols, values_

    def _packing_query(self) -> Sequence:
        ...

    def execute_select(self, table,
                       columns: [Iterable[str], str] = "*",
                       sorted_: str = None,
                       distinct: bool = False,
                       condition: str = "",
                       first: bool = False):
        if columns != "*":
            columns = ", ".join(columns)[1:]
        if condition:
            condition = f"{SQLCommand.where.value} {condition}"
        if not sorted_:
            sorted_ = ""
        if not first:
            first = ""
        else:
            first = "LIMIT 1"

        self.execute(f"{SQLCommand.select.value} {columns} FROM {table} {sorted_} {condition} {first} ;")

        return self._packing_query()

    def execute_create_db(self, name: str):
        self.execute(f"CREATE DATABASE {name};")
        self.db.database = name

    def execute_drop_db(self, name: str):
        ...

    def execute_insert(self, table, columns: tuple, values: tuple):
        columns, values = self._adding_quot(values, columns)
        cols = f'({str(",").join(columns)})'
        vals = f'({str(",").join(values)})'

        self._cursor.execute(f"{SQLCommand.insert.value} {SQLCommand.into.value} {table}"
                             f" {cols} VALUES {vals};")

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._is_conn:
            self._cursor.close()
            self.db.close()
            self._is_conn = False

    def start(self):
        return self.__enter__()

    def execute_create_table(self, name: str, columns: tuple, primary, foreign_key,
                             reference: tuple = (),
                             on_delete: bool=False, on_update:bool=False):

        primary = f", PRIMARY KEY ({primary}) " if primary else ""
        foreign_key = f", FOREIGN KEY ({foreign_key})"   if foreign_key else ""
        ondelete = " ON DELETE CASCADE" if on_delete else ""
        onupdate = " ON UPDATE CASCADE"   if on_update else ""
        reference = f" REFERENCES {reference[0]}({reference[1]}){ondelete}{onupdate}"   if reference else ""

        self.execute(f"CREATE TABLE IF NOT EXISTS {name} ({str(',').join(columns)}{primary}{foreign_key}{reference});")

    def stop(self):
        self.__exit__(None, None, None)

    def execute_delete_by(self, table, column, value):
        self.execute(f"DELETE FROM {table} WHERE {column} = \"{value}\";")

    def execute_delete_if_equal(self, table, statement):

        self.execute(f"DELETE FROM {table} WHERE {statement};")

    def execute_drop_table(self, table: str):
        self.execute(f"DROP TABLE IF EXISTS {table}")

    def execute_increment_value(self, name: str, val: int):
        ...

    def execute_backup(self, database: str, filepath: str, diff: bool = False):
        if diff:
            diff_ = " WITH DIFFERENTIAL"
        else:
            diff_ = ""
        self.execute(f"BACKUP DATABASE {database} TO DISK = '{filepath}'{diff_};")

    def execute_update_table(self, table, data,prime_indexes:str=None, condition=None,
                             filters: list[tuple] = None, foreign_key=False):
        if foreign_key:
            self.execute("PRAGMA foreign_keys = ON;")
        if not filters:
            res = []
            for c, v in data.__dict__.items():
                if c == prime_indexes:
                    continue
                if dict == type(v):

                    res.append(f"{c} = \'{json.dumps({'dict': v})}\'")
                elif list == type(v):
                    res.append(f"{c} = \'{json.dumps({'list': v})}\'")
                else:
                    res.append(f"{c} = \'{v}\'")

            filters_parse = res
        else:

            filters_parse = [f"{c} = \'{v}\'" for c, v in data.__dict__.items() if data.__dict__[c] != v]


        if condition:
            condition = f" WHERE {condition}"
        else:

            condition = f" WHERE {filters_parse.pop(0)}"


        self.execute(f"UPDATE {table} SET {','.join(filters_parse)}{condition};")

    def execute(self, statement):
        self._cursor.execute(statement)

    def databases(self):
        ...


class SQLServerLess(SQLExecutor):
    def __init__(self, *args, **kwargs):

        import sqlite3
        super().__init__(*args, **kwargs)
        self.db = sqlite3.connect(*args, **kwargs)
        self._cursor = self.db.cursor()
        self._is_conn = True

    def execute_insert(self, table, columns: tuple, values: tuple):
        self.execute("PRAGMA foreign_keys = ON;")
        super(SQLServerLess, self).execute_insert(table, columns, values)

    def execute_delete_by(self, table, column, value):
        self.execute("PRAGMA foreign_keys = ON;")
        super(SQLServerLess, self).execute_delete_by(table, column, value)

    def execute_delete_if_equal(self, table, statement):
        self.execute("PRAGMA foreign_keys = ON;")
        super(SQLServerLess, self).execute_delete_if_equal(table, statement)

    def execute_create_table(self, name: str, columns: tuple, primary, foreign_key: str = "",
                             reference: tuple = None,on_delete=False,on_update=False):
        if foreign_key:
            self.execute("PRAGMA foreign_keys = ON;")

        super(SQLServerLess, self).execute_create_table(name, columns, primary,
                                                        foreign_key, reference,
                                                        on_delete,on_update)

    def _packing_query(self):
        names = list(map(lambda x: x[0], self._cursor.description))
        res = []
        for cols in self._cursor.fetchall():
            t = DBTable()

            for i, col in enumerate(names):
                if type(cols[i]) == str:
                    if '{"list": ' in cols[i]:
                        t[col] = json.loads(cols[i])['list']
                    elif '{"dict": ' in cols[i]:
                        t[col] = json.loads(cols[i])['dict']
                    else:
                        t[col] = cols[i]
                else:
                    t[col] = cols[i]

            res.append(t)
        return res

    def execute_increment_value(self, name: str, val: int):
        self.execute(f"ALTER TABLE {name} AUTOINCREMENT={val};")

    def databases(self):
        res = []
        for f in os.listdir():
            if f.endswith(".db"):
                res.append((f,))
        return res

    def execute_drop_db(self, name: str):
        self.stop()
        os.remove(name + ".db")


class SQLServer(SQLExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if kwargs.get("create_and_ignore", None):
            self._auto_create_and_ignore(*args, **kwargs)
        else:
            self._create(*args, **kwargs)
        self._is_conn = True

    def execute_drop_db(self, name: str):
        self.execute(f"DROP DATABASE {name};")

    def execute_increment_value(self, name: str, val: int):
        self.execute(f"ALTER TABLE {name} AUTO_INCREMENT={val};")

    def _auto_create_and_ignore(self, *args, **kwargs):
        name = kwargs.get("database", None)
        if name:
            kwargs["database"] = None
        self._create(*args, **kwargs)
        if name not in [dbs[0] for dbs in self.databases()]:
            self.execute_create_db(name)
            self.db.database = name
        else:
            self.db.database = name

    def _create(self, *args, **kwargs):
        import mysql.connector
        kwargs.pop("create_and_ignore", None)
        self.db = mysql.connector.connect(*args, **kwargs)
        self._cursor = self.db.cursor()

    def _packing_query(self):
        res = []

        for cols in self._cursor.fetchall():
            t = DBTable()
            for i, col in enumerate(self._cursor.column_names):

                if type(cols[i]) == str:
                    if '{"list": ' in cols[i]:
                        t[col] = json.loads(cols[i])['list']
                    elif '{"dict": ' in cols[i]:
                        t[col] = json.loads(cols[i])['dict']
                    else:
                        t[col] = cols[i]
                else:
                    t[col] = cols[i]
            res.append(t)
        return res

    def databases(self) -> list:
        self.execute("show databases")
        return self._cursor.fetchall()


class SQLTypes:

    def __init__(self, serverless=False):
        self._server_less = serverless

    @staticmethod
    def text(size: int = 0, long: bool = False):
        if long:
            return f"LONGTEXT"
        return f"TEXT({size})"

    @staticmethod
    def boolean():
        return "BOOL"

    @staticmethod
    def double(size: int, d: int):
        return f"DOUBLE({size}, {d})"

    @staticmethod
    def char(size: int):
        return f"CHAR({size})"

    @staticmethod
    def objType(max_size: int = None):
        if max_size:
            return SQLTypes.text(max_size)
        return SQLTypes.text(long=True)

    @staticmethod
    def integer():
        return f"INTEGER"

    @staticmethod
    def image(max:int=100):
        return f"VARBINARY({max})"


    @staticmethod
    def varchar(size: int, ):
        return f"VARCHAR({size})"

    def column(self, d_type, nullable: bool = True, auto_increment: bool = False,unique=False):
        if nullable:
            nullable = ""
        else:
            nullable = " NOT NULL"
        if auto_increment:
            if self._server_less:
                auto_increment = ""
            else:
                auto_increment = " AUTO_INCREMENT"

        else:
            auto_increment = ""
        if unique:
            unique = " UNIQUE"
        else:
            unique = ""
        return f"{d_type}{nullable}{auto_increment}{unique}"


class SimpleSQL:
    AUTO_INC = "AUTO_INC_VALUE"

    def __init__(self, executor: SQLExecutor = None):
        self._executor = executor
        if isinstance(executor, SQLServer):
            self._types = SQLTypes()
        else:
            self._types = SQLTypes(True)

    @property
    def executor(self) -> SQLExecutor:
        return self._executor

    @property
    def types(self) -> SQLTypes:
        return self._types

    def commit(self):
        self._executor.db.commit()

    def set_auto_commit(self, val: bool):
        self._executor.db.autocommit = val

    def update_column_to_date(self,table,column,default = False,on_update=False):
        self._executor.execute(f"""ALTER TABLE {table}
    CHANGE {column}
        {column} TIMESTAMP NOT NULL
            {"DEFAULT CURRENT_TIMESTAMP" if default else ""}
            {"ON UPDATE CURRENT_TIMESTAMP" if column else ""};""")

    def drop_database(self, name):
        dbs = self.local_databases()
        if name not in dbs and f"{name}.db" not in dbs:
            raise DatabaseNotExist(f"you cant drop none exists database named \"{name}\"")
        self._executor.execute_drop_db(name)

    def create_database(self, name):
        if self._types._server_less:
            raise Exception("cant create db with serverless like that")
        if name not in self.local_databases():

            self._executor.execute_create_db(name)
        else:
            raise DatabaseExist(f"database named \"{name}\" is already created.")

    def create_table(self, table: type, data, primary_key: str = None,
                     auto_increment_value: int = None,
                     foreign_key: str = None,
                     reference: tuple = None,
                     ondelete=None,onupdate=None):

        self._executor.execute_create_table(table.__name__,
                                                tuple([f"{obj} {type_}" for obj, type_ in data.__dict__.items()]),
                                                primary_key,foreign_key,
                                                reference=reference,on_delete=ondelete,on_update=onupdate
                                              )


        if auto_increment_value and not self._types._server_less:
            self._executor.execute_increment_value(table.__name__, auto_increment_value)

    def query_filters(self, table: type, filters: str, first: bool = False):
        result = self._executor.execute_select(table.__name__, condition=filters)
        if not result:
            return None
        return [table(**item.__dict__) for item in result] if not first else table(**result[0].__dict__)

    def query_filter_by(self, table: type, filter_: str, filter_value: Any, first=False):
          if type(filter_value) == list:
            filter_value=json.dumps({"list":filter_value})
        elif type(filter_value) == dict:
            filter_value = json.dumps({"dict": filter_value})
        result = self._executor.execute_select(table.__name__, condition=f"{filter_} = \'{filter_value}\'", first=first)
        if not result:
            return None
        return [table(**item.__dict__) for item in result] if not first else table(**result[0].__dict__)

    def query_all(self, table: type):
        result = self._executor.execute_select(table.__name__)

        return [table(**item.__dict__) for item in result]

    def insert_to(self, table: type, data):
        self._executor.execute_insert(table.__name__, tuple(data.__dict__.keys()), tuple(data.__dict__.values()))

    def query_ordered(self, table: type, key: str, reverse: bool = False):
        if key:
            key = f"{SQLCommand.order.value} {key}"
        result = self._executor.execute_select(table.__name__, sorted_=key)
        return [table(**item.__dict__) for item in result]

    def query_delete_by(self, table: type, filter_by: tuple[str, Any]):
        self._executor.execute_delete_by(table.__name__, filter_by[0], filter_by[1])

    def drop_table(self, table: Union[str, type]):
        self._executor.execute_drop_table(table.__name__ if not isinstance(table, str) else table)

    def local_databases(self) -> list:
        return [db[0] for db in self._executor.databases()]

    def query_update_table(self, table, data,prime_indexes=0,foreign_key = False):
        self._executor.execute_update_table(table.__name__, data,prime_indexes=prime_indexes,condition=None,
                                            filters=None,
                                            foreign_key=foreign_key)

    def query_alter_table_forgkey(self, table, foreign_key, reference: tuple, ondelete="", onupdate=""):
        if ondelete:
            ondelete = " ON DELETE CASCADE"
        if onupdate:
            onupdate = " ON UPDATE CASCADE"
        self._executor.execute \
            (f"ALTER TABLE {table} ADD FOREIGN KEY ({foreign_key}) REFERENCES "
             f"{reference[0]}({reference[1]}){ondelete}{onupdate};")

    def backup(self, filepath: str, diff: bool = False):
        if self._executor.db.database:
            self._executor.execute_backup(self._executor.db.database, filepath, diff)
        else:
            raise DatabaseNotExist(f"connected with {self._executor.db.database} database."
                                   f"consider to connect or created database to backup,"
                                   f" or just use executor.execute_backup()")

    def add(self, instance: Any):
        temp = instance.__dict__
        self.create_table(*self._prepare_table(instance))
        self.insert_to(type(instance), type(instance)(**temp))

    def delete(self, instance: Any):
        r = ""
        for k, v in instance.__dict__.items():
            r += f" {k} = \"{v}\" AND"
        self._executor.execute_delete_if_equal(type(instance).__name__, r[:-3:])

    def _prepare_table(self, instance):
        table_name = type(instance).__name__
        temp = {}
        primary = None

        for attribute, value in instance.__dict__.items():
            if value == self.AUTO_INC:
                primary = attribute
                temp[attribute] = self._types.column(self.types.integer(), auto_increment=True)
                continue

            if isinstance(value, int):

                temp[attribute] = self._types.column(self._types.integer(), )

            elif isinstance(value, (list, tuple, dict)):
                temp[attribute] = self._types.column(self._types.objType(), )
            elif isinstance(value, str):
                temp[attribute] = self._types.column(self._types.varchar(50), )
            else:
                temp[attribute] = None

        instance.__dict__ = temp
        return table_name, instance, primary


def connect(serverless=False, create_and_ignore=False, *args, **kwargs) -> SQLExecutor:
    if serverless:
        return SQLServerLess(*args, **kwargs)
    if create_and_ignore:
        kwargs["create_and_ignore"] = create_and_ignore
        return SQLServer(*args, **kwargs)
    return SQLServer(*args, **kwargs)

#todo fix CHANGE command in serverless



