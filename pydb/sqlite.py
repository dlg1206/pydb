"""
File: sqlite.py
Description: Sqlite database interface for handling CRUD methods data

@author Derek Garcia
"""

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from sqlite3 import Connection, Cursor, connect, OperationalError, IntegrityError
from typing import List, Tuple, Any, Generator, Dict

from pydb.common.base_table import BaseTable


class SQLiteDatabase:
    """
    Generic interface for accessing a SQLite Database
    """

    def __init__(self, db_path: str, ddl_directory: str = None, rebuild: bool = False):
        """
        Open interface to SQL db. If it doesn't exist, create new database

        :param db_path: Path to the sqlite database
        :param ddl_directory: Path to DDL to construct the database (default: None)
        :param rebuild: Rebuild the database (default: False)
        """
        self._db_path = db_path if db_path.endswith('.db') else f"{db_path}.db"  # ensure '.db' file ext

        # If database exists and no rebuild, don't rebuild
        if os.path.exists(self._db_path) and not rebuild:
            logging.debug(f"Using database at {self._db_path}")
            return

        # delete old iff exists
        if rebuild and os.path.exists(self._db_path):
            logging.warning("Force rebuilding the database")
            logging.debug(f"Deleting database: {self._db_path}")
            os.remove(self._db_path)

        # Init db
        logging.debug(f"Creating database at '{self._db_path}'")
        db_dir = os.path.dirname(self._db_path)
        Path(db_dir).mkdir(parents=True, exist_ok=True)

        # if no ddl to read, return
        if not ddl_directory:
            return
        with self.connection() as conn:
            self._init_database(ddl_directory, conn)

    def _init_database(self, ddl_directory: str, conn: Connection) -> None:
        """
        Init a database from DDL files

        :param ddl_directory: Directory to load DDL from
        :param conn: Connection to database
        """
        # validate ddl directory
        if not os.path.exists(ddl_directory):
            raise FileNotFoundError(ddl_directory)
        if not os.path.isdir(ddl_directory):
            raise ValueError(f"DDL path is not a directory: {ddl_directory}")

        # recursive parse ddl directory
        with self.cursor(conn) as cur:
            for root, dirs, files in os.walk(ddl_directory):
                for file in files:
                    # skip any non sql files
                    if not (file.endswith('.sql')):
                        continue
                    # Execute sql file
                    with open(os.path.join(root, file), 'r') as sql_file:
                        logging.debug(f"Loading {sql_file}")
                        cur.executescript(sql_file.read())
        logging.debug(f"Created database at {self._db_path}")

    def insert(self,
               table: BaseTable,
               inserts: Dict[str, Any],
               on_success_msg: str = None) -> None:
        """
        Generic insert into the database

        :param table: Table to insert into
        :param inserts: Values to insert (column, value)
        :param on_success_msg: Optional debug message to print on success (default: nothing)
        :raises IntegrityError: if insert violates table rules
        :raises OperationalError: if fail to insert
        """
        # build sql
        columns = list(inserts.keys())
        values = list(inserts.values())
        columns_sql = f"({', '.join(columns)})" if columns else ''  # ( c1, ..., cN )
        params_sql = f"({', '.join('?' for _ in values)})"  # ( ?, ..., N )
        sql = f"INSERT INTO {table.value} {columns_sql} VALUES {params_sql};"

        with self.connection() as conn:
            with self.cursor(conn) as cur:
                # execute
                try:
                    cur.execute(sql, values)
                    conn.commit()
                except IntegrityError as ie:
                    # duplicate entry
                    logging.debug(f"{ie.sqlite_errorname} | {table.value} | ({values})")
                    raise ie
                except OperationalError as oe:
                    # failed to insert
                    logging.error(oe)
                    raise oe
        # print success message if given one
        if on_success_msg:
            logging.debug(on_success_msg)

    def select(self,
               table: BaseTable,
               columns: List[str] = None,
               where_equals: Dict[str, Any] = None,
               fetch_all: bool = True) -> List[Tuple[Any]]:
        """
        Generic select from the database

        :param table: Table to select from
        :param columns: optional column names to insert into (default: *)
        :param where_equals: optional where equals clause (column, value)
        :param fetch_all:
            Fetch all rows, fetch one if false.
            Useful if checking to table contains value (Default: True)
        """

        # build SQL
        columns_names = f"{', '.join(columns)}" if columns else '*'  # ( c1, ..., cN )
        sql = f"SELECT {columns_names} FROM {table.value}"

        # add where clauses if given
        params = []
        if where_equals:
            where_clause, params = _build_where_clause(where_equals)
            sql += where_clause

        with self.connection() as conn:
            with self.cursor(conn) as cur:
                # execute with where params if present
                cur.execute(sql, params)
                return cur.fetchall() if fetch_all else cur.fetchone()

    def update(self,
               table: BaseTable,
               updates: Dict[str, Any],
               where_equals: Dict[str, Any] = None,
               on_success: str = None,
               amend: bool = False) -> int:
        """
        Generic update from the database

        :param table: Table to select from
        :param updates: list of updates to the table (column, value)
        :param where_equals: optional where equals clause (column, value)
        :param on_success: Optional debug message to print on success (default: nothing)
        :param amend: Amend to row instead of replacing (default: False)
        :raises OperationalError: if fail to update row
        :return: True if update, false otherwise
        """
        # reject if no updates
        if not updates:
            return 0

        # Build SET clause
        if amend:
            set_clause = ', '.join(f"{col} = {col} || ?" for col in updates)
        else:
            set_clause = ', '.join(f"{col} = ?" for col in updates)
        sql = f"UPDATE {table.value} SET {set_clause}"
        params = list(updates.values())

        # Add WHERE clause
        if where_equals:
            where_clause, where_params = _build_where_clause(where_equals)
            sql += where_clause
            params.extend(where_params)

        with self.connection() as conn:
            with self.cursor(conn) as cur:
                try:
                    # execute
                    cur.execute(sql, params)
                    conn.commit()
                except OperationalError as oe:
                    # failed to update
                    logging.error(oe)
                    raise oe
                rowcount = cur.rowcount
        # print success message if given one
        if rowcount > 0 and on_success:
            logging.debug(on_success)
        return rowcount  # rows changed

    def upsert(self,
               table: BaseTable,
               primary_keys: Dict[str, Any],
               updates: Dict[str, Any],
               print_on_success: bool = True) -> None:
        """
        Generic upsert to the database

        :param table: Table to select from
        :param primary_keys: Primary key(s) to update (column, value)
        :param updates: list of updates to the table (column, value)
        :param print_on_success: Print debug message on success (default: True)
        """
        # attempt to update
        msg = None
        if print_on_success:
            msg = ", ".join([f"{k} '{v}'" for k, v in primary_keys.items()])

        updated = self.update(table, updates,
                              where_equals=primary_keys,
                              on_success=f"Updated {msg}" if print_on_success else None,
                              amend=False)

        if not updated:
            # if fail, insert
            updates.update(primary_keys)
            self.insert(table, updates, on_success_msg=f"Inserted {msg}" if print_on_success else None)

    @contextmanager
    def connection(self) -> Generator[Connection, Any, None]:
        """
        Open a connection that can be closed on exit

        :return: Database connection
        """
        conn = None
        try:
            conn = connect(self._db_path)
            yield conn
        except OperationalError as oe:
            logging.critical(oe)
        finally:
            if conn:
                conn.rollback()  # clear any uncommitted transactions
                conn.close()

    @contextmanager
    def cursor(self, connection: Connection) -> Generator[Cursor, Any, None]:
        """
        Open a cursor that can be closed on exit

        :param connection: Database connection to get the cursor for
        :return: cursor
        """
        cur = None
        try:
            cur = connection.cursor()
            cur.execute('PRAGMA foreign_keys = ON;')  # enforce foreign keys
            yield cur
        except OperationalError as oe:
            logging.critical(oe)
        finally:
            if cur:
                cur.close()


def _build_where_clause(where_equals: Dict[str, Any]) -> Tuple[str, List[Any]]:
    """
    Build a sqlite SQL where clause

    :param where_equals: Where equals clause (column, value)
    :return: Where equals clause string, list of params
    """
    params = []
    clauses = []
    for column, value in where_equals.items():
        if value is None:
            clauses.append(f"{column} IS NULL")
        else:
            clauses.append(f"{column} = ?")
            params.append(value)
    return " WHERE " + " AND ".join(clauses), params
