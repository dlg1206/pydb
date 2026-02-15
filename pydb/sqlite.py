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
from typing import List, Tuple, Any, Generator

from pydb.common.table import Table


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

    def insert(self, table: Table, inserts: List[Tuple[str, str | int]], on_success_msg: str = None) -> None:
        """
        Generic insert into the database

        :param table: Table to insert into
        :param inserts: Values to insert (column, value)
        :param on_success_msg: Optional debug message to print on success (default: nothing)
        """
        with self.connection() as conn:
            with self.cursor(conn) as cur:
                columns, values = zip(*[(e[0], e[1]) for e in inserts])  # unpack inserts
                columns = list(columns)
                values = list(values)
                # build SQL
                columns_names = f"({', '.join(columns)})" if columns else ''  # ( c1, ..., cN )
                params = f"({', '.join('?' for _ in values)})"  # ( ?, ..., N )
                sql = f"INSERT INTO {table.value} {columns_names} VALUES {params};"

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

    def select(self, table: Table, columns: List[str] = None,
               where_equals: List[Tuple[str, str | int]] = None) \
            -> List[Tuple[str | int]]:
        """
        Generic select from the database

        :param table: Table to select from
        :param columns: optional column names to insert into (default: *)
        :param where_equals: optional where equals clause (column, value)
        """
        with self.connection() as conn:
            with self.cursor(conn) as cur:
                # build SQL
                columns_names = f"{', '.join(columns)}" if columns else '*'  # ( c1, ..., cN )
                sql = f"SELECT {columns_names} FROM {table.value}"
                # add where clauses if given
                if where_equals:
                    sql += ' WHERE ' + ' AND '.join(
                        [f"{clause[0]} = ?" for clause in where_equals])  # append all where's

                # execute with where params if present
                cur.execute(f"{sql};", [] if not where_equals else [clause[1] for clause in where_equals])
                return cur.fetchall()

    def update(self, table: Table, updates: List[Tuple[str, str | int]],
               where_equals: List[Tuple[str, str | int]] = None, on_success: str = None, amend: bool = False) -> bool:
        """
        Generic update from the database

        :param table: Table to select from
        :param updates: list of updates to the table (column, value)
        :param where_equals: optional where equals clause (column, value)
        :param on_success: Optional debug message to print on success (default: nothing)
        :param amend: Amend to row instead of replacing (default: False)
        :return: True if update, false otherwise
        """
        with self.connection() as conn:
            with self.cursor(conn) as cur:
                # build SQL
                if amend:
                    set_clause = ', '.join(f"{col} = {col} || (?)" for col, _ in updates)
                else:
                    set_clause = ', '.join(f"{col} = (?)" for col, _ in updates)

                values = [u[1] for u in updates]
                sql = f"UPDATE {table.value} SET {set_clause}"

                # add where clauses if given
                if where_equals:
                    sql += ' WHERE ' + ' AND '.join(
                        [f"{clause[0]} = ?" for clause in where_equals])  # append all where's
                    [values.append(clause[1]) for clause in where_equals]
                # execute
                try:
                    # execute with where params if present
                    cur.execute(f"{sql};", values)
                    conn.commit()
                except OperationalError as oe:
                    # failed to update
                    logging.error(oe)
                    raise oe
                # print success message if given one
                if cur.rowcount > 0 and on_success:
                    logging.debug(on_success)
                return cur.rowcount > 0  # rows changed

    def upsert(self, table: Table, primary_key: Tuple[str, str], updates: List[Tuple[str, str]],
               print_on_success: bool = True) -> None:
        """
        Generic upsert to the database

        :param table: Table to select from
        :param primary_key: Primary key to update (column, value)
        :param updates: list of updates to the table (column, value)
        :param print_on_success: Print debug message on success (default: True)
        """
        # attempt to update
        if not self.update(table, updates,
                           where_equals=[primary_key],
                           on_success=f"Updated {primary_key[0]} '{primary_key[1]}'" if print_on_success else None,
                           amend=True):
            # if fail, insert
            updates.append(primary_key)
            self.insert(table, updates,
                        on_success_msg=f"Inserted {primary_key[0]} '{primary_key[1]}'" if print_on_success else None)
