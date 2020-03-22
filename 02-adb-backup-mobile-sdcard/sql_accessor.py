# Version 1.4
# CHANGELOG
# ver 1.4: added mysql support
# ver 1.3: added future support for generic sql accessor class
# ver 1.2: providing global lock for multi-threaded access
# ver 1.1: enabled strict foreign key, create table sequence order is controlled by foreign key constraints now
import os
import re
import orm_utils
import sql_autogenerator
from typing import Type, Optional, Any
import threading

create_table_stmt = re.compile(r'create\s+table\s+`?(?P<table_name>[a-zA-Z0-9_]+)`?\s*', re.IGNORECASE)


def _create_table_not_exists(sql_stmt, cursor, check_table_fn):
    match = re.search(create_table_stmt, sql_stmt)
    if match is None:
        raise ValueError('Invalid create table statement: %s' % sql_stmt)
    if not check_table_fn(cursor, match.group('table_name')):
        cursor.execute(sql_stmt)


class _FakeLock:
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def acquire(self, *args, **kwargs):
        pass

    def release(self, *args, **kwargs):
        pass


class GenericSqlAccessor:
    def __init__(self, sql_generator: sql_autogenerator.AbstractSqlStatementGenerator, connection: Any,
                 ensure_thread_safe: bool = True):
        self._generator = sql_generator
        self._connection = connection
        self._global_lock = threading.RLock() if ensure_thread_safe else _FakeLock()
        self._checked_existed_tables = set()

    def _table_exists(self, cursor: Any, table_name: str) -> bool:
        raise NotImplementedError

    def _create_table_dependency_order(self, cursor, entity_class):
        if entity_class in self._checked_existed_tables:
            return  # reduce repeat leaves
        for descriptor in entity_class.__FIELDS__:
            if type(descriptor) == orm_utils.ForeignKeyDescriptor:
                table_name = descriptor.ref_table_name
                depend_entity_class = [x for x in orm_utils.Entity.EntityClass if x.__TABLE_NAME__ == table_name]
                assert len(depend_entity_class) == 1, 'Table %s is not mapped to any subclass of Entity' % table_name
                self._create_table_dependency_order(cursor, depend_entity_class[0])
        if not self._table_exists(cursor, entity_class.__TABLE_NAME__):
            self._generator.create_table(entity_class, cursor)
        self._checked_existed_tables.add(entity_class)

    def insert(self, entity: orm_utils.Entity):
        with self._global_lock:
            cursor = self._connection.cursor()
            self._create_table_dependency_order(cursor, type(entity))
            self._generator.insert(entity, cursor)
            cursor.close()

    def update(self, entity: orm_utils.Entity):
        with self._global_lock:
            cursor = self._connection.cursor()
            self._create_table_dependency_order(cursor, type(entity))
            self._generator.update(entity, cursor)
            cursor.close()

    def select(self, entity: Type[orm_utils.Entity], fetch_count: int, **keys):
        with self._global_lock:
            cursor = self._connection.cursor()
            self._create_table_dependency_order(cursor, entity)
            result = self._generator.select(entity, cursor, fetch_count, **keys)
            cursor.close()
            return result

    def delete(self, entity: Type[orm_utils.Entity], **keys):
        with self._global_lock:
            cursor = self._connection.cursor()
            self._create_table_dependency_order(cursor, entity)
            self._generator.delete(entity, cursor, **keys)
            cursor.close()

    def commit(self):
        with self._global_lock:
            self._connection.commit()

    def cursor(self):
        return self._connection.cursor()

    def get_variable(self, key: str, default: Optional[str] = None) -> str:
        raise NotImplementedError

    def set_variable(self, key: str, value: str):
        raise NotImplementedError

    def delete_variable(self, key: str):
        raise NotImplementedError


class SqliteAccessor(GenericSqlAccessor):
    def __init__(self, sqlite_path: str, ensure_thread_safe: bool = True):
        import sqlite3
        if os.path.exists(sqlite_path):
            assert os.path.isfile(sqlite_path)
        connection = sqlite3.connect(sqlite_path, check_same_thread=False,
                                     detect_types=sqlite3.PARSE_COLNAMES | sqlite3.PARSE_DECLTYPES)
        generator = sql_autogenerator.SqliteSqlStatementGenerator()
        super(SqliteAccessor, self).__init__(generator, connection, ensure_thread_safe)
        self._check_tables()

    def _check_tables(self):
        cursor = self._connection.cursor()
        cursor.execute('pragma foreign_keys = on')
        _create_table_not_exists("create table db_vars(key varchar(255) primary key not null unique, "
                                 "value text)", cursor, self._table_exists)
        cursor.close()
        self._connection.commit()

    def _table_exists(self, cursor: Any, table_name: str) -> bool:
        cursor.execute("select count(1) from sqlite_master where name = ? and type = 'table'", (table_name,))
        return cursor.fetchone()[0] > 0

    def get_variable(self, key: str, default: Optional[str] = None) -> str:
        assert type(key) == str
        with self._global_lock:
            cursor = self._connection.cursor()
            cursor.execute("select value from db_vars where key = ?", (key,))
            results = cursor.fetchall()
            cursor.close()
            if len(results) == 0:
                if default is None:
                    raise KeyError('Key %s not found' % key)
                else:
                    return default
            return results[0][0]

    def set_variable(self, key: str, value: str):
        assert type(key) == str and type(value) == str
        with self._global_lock:
            cursor = self._connection.cursor()
            cursor.execute("select count(1) from db_vars where key = ?", (key,))
            if cursor.fetchone()[0] == 0:
                cursor.execute("insert into db_vars(key, value) values (?, ?)", (key, value))
            else:
                cursor.execute("update db_vars set value = ? where key = ?", (value, key))
            cursor.close()

    def delete_variable(self, key: str):
        with self._global_lock:
            cursor = self._connection.cursor()
            cursor.execute("delete from db_vars where key = ?", (key,))
            cursor.close()


class MysqlAccessor(GenericSqlAccessor):
    def __init__(self, host: str, user: str, password: str, database: Optional[str] = None,
                 ensure_thread_safe: bool = True, **kwargs):
        import mysql.connector
        connection = mysql.connector.connect(host=host, user=user, password=password, database=database, **kwargs)
        generator = sql_autogenerator.MysqlSqlStatementGenerator()
        super(MysqlAccessor, self).__init__(generator, connection, ensure_thread_safe)

        cursor = self._connection.cursor()
        _create_table_not_exists("create table `db_vars`(`key` varchar(255) primary key not null unique, "
                                 "`value` text)", cursor, self._table_exists)
        cursor.close()
        self._connection.commit()

    def _table_exists(self, cursor: Any, table_name: str) -> bool:
        cursor.execute('show tables like %s', (table_name,))
        return len(cursor.fetchall()) > 0

    def get_variable(self, key: str, default: Optional[str] = None) -> str:
        assert type(key) == str
        with self._global_lock:
            cursor = self._connection.cursor()
            cursor.execute("select `value` from `db_vars` where `key` = %s", (key,))
            results = cursor.fetchall()
            cursor.close()
            if len(results) == 0:
                if default is None:
                    raise KeyError('Key %s not found' % key)
                else:
                    return default
            return results[0][0]

    def set_variable(self, key: str, value: str):
        assert type(key) == str and type(value) == str
        with self._global_lock:
            cursor = self._connection.cursor()
            cursor.execute("select count(1) from `db_vars` where `key` = %s", (key,))
            if cursor.fetchone()[0] == 0:
                cursor.execute("insert into `db_vars`(`key`, `value`) values (%s, %s)", (key, value))
            else:
                cursor.execute("update `db_vars` set `value` = %s where `key` = %s", (value, key))
            cursor.close()

    def delete_variable(self, key: str):
        with self._global_lock:
            cursor = self._connection.cursor()
            cursor.execute("delete from `db_vars` where `key` = %s", (key,))
            cursor.close()
