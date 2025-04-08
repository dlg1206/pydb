import logging
import os
from contextlib import contextmanager
from typing import List, Any, Tuple

from dotenv import load_dotenv
from mysql import connector
from mysql.connector import pooling, IntegrityError
from mysql.connector.pooling import PooledMySQLConnection

from common.entity import Table

DEFAULT_POOL_SIZE = 32

"""
File: mysql_db.py
Description: MySQL database interface for handling CRUD methods data

@author Derek Garcia
"""


class MySQLDatabase:
    """
    Generic interface for accessing a SQL Database
    """

    def __init__(self, pool_size: int = DEFAULT_POOL_SIZE):
        """
        Create MySQL interface and connection pool to use. Uses environment variables for credentials
        """
        # todo - best way to handle this?
        load_dotenv()
        db_config = {
            "user": os.getenv("MYSQL_USER"),
            "password": os.getenv("MYSQL_PASSWORD"),
            "host": os.getenv("MYSQL_HOST"),
            "database": os.getenv("MYSQL_DATABASE"),
            "port": int(os.getenv("EXTERNAL_PORT"))
        }

        self._connection_pool = pooling.MySQLConnectionPool(pool_size=pool_size, **db_config)

    @contextmanager
    def _open_connection(self) -> PooledMySQLConnection:
        """
        Open a connection from the pool that will be closed on exit

        :return: Database connection
        """
        conn = None
        try:
            conn = self._connection_pool.get_connection()
            yield conn
        except connector.Error as oe:
            logging.critical(oe)
        finally:
            if conn:
                conn.rollback()  # clear any uncommitted transactions
                conn.close()

    @contextmanager
    def _get_cursor(self, connection: PooledMySQLConnection) -> MySQLCursor:
        """
        Open a cursor that can be closed on exit

        :param connection: Database connection to get the cursor for
        :return: cursor
        """
        cur = None
        try:
            cur = connection.cursor()
            yield cur
        except connector.Error as oe:
            logging.critical(oe)
        finally:
            if cur:
                cur.close()

    def _insert(self, table: Table, inserts: List[Tuple[str, Any]], on_success_msg: str = None) -> int | None:
        """
        Generic insert into the database

        :param table: Table to insert into
        :param inserts: Values to insert (column, value)
        :param on_success_msg: Optional debug message to print on success (default: nothing)
        :return: Autoincrement id of inserted row if used, else None
        """
        with self._open_connection() as conn:
            with self._get_cursor(conn) as cur:
                columns, values = zip(*[(e[0], e[1]) for e in inserts])  # unpack inserts
                columns = list(columns)
                values = list(values)
                # build SQL
                columns_names = f"({', '.join(columns)})" if columns else ''  # ( c1, ..., cN )
                params = f"({', '.join('%s' for _ in values)})"  # ( %s, ..., N )
                sql = f"INSERT INTO {table.value} {columns_names} VALUES {params};"
                # execute
                try:
                    cur.execute(sql, values)
                    conn.commit()
                    # print success message if given one
                    if on_success_msg:
                        logging.debug(on_success_msg)
                except IntegrityError as ie:
                    # duplicate entry
                    logging.debug(f"{ie.errno} | {table.value} | ({', '.join(values)})")
                    pass
                except connector.Error as oe:
                    # failed to insert
                    logging.error(oe)
                    return None
                # return auto incremented id if used
                return cur.lastrowid

    def _select(self, table: Table, columns: List[str] = None,
                where_equals: List[Tuple[str, Any]] = None) \
            -> List[Tuple[Any]]:
        """
        Generic select from the database

        :param table: Table to select from
        :param columns: optional column names to insert into (default: *)
        :param where_equals: optional where equals clause (column, value)
        """
        with self._open_connection() as conn:
            with self._get_cursor(conn) as cur:
                # build SQL
                columns_names = f"{', '.join(columns)}" if columns else '*'  # c1, ..., cN
                sql = f"SELECT {columns_names} FROM {table.value}"
                # add where clauses if given
                if where_equals:
                    sql += ' WHERE ' + ' AND '.join(
                        [f"{clause[0]} = %s" for clause in where_equals])  # append all where's

                # execute with where params if present
                cur.execute(f"{sql};", [] if not where_equals else [clause[1] for clause in where_equals])
                return cur.fetchall()

    def _update(self, table: Table, updates: List[Tuple[str, Any]],
                where_equals: List[Tuple[str, Any]] = None, on_success: str = None, amend: bool = False) -> bool:
        """
        Generic update from the database

        :param table: Table to select from
        :param updates: list of updates to the table (column, value)
        :param where_equals: optional where equals clause (column, value)
        :param on_success: Optional debug message to print on success (default: nothing)
        :param amend: Amend to row instead of replacing (default: False)
        :return: True if update, false otherwise
        """
        with self._open_connection() as conn:
            with self._get_cursor(conn) as cur:
                # build SQL
                if amend:
                    set_clause = ', '.join(f"{col} = {col} || (%s)" for col, _ in updates)
                else:
                    set_clause = ', '.join(f"{col} = (%s)" for col, _ in updates)

                values = [u[1] for u in updates]
                sql = f"UPDATE {table.value} SET {set_clause}"

                # add where clauses if given
                if where_equals:
                    sql += ' WHERE ' + ' AND '.join(
                        [f"{clause[0]} = %s" for clause in where_equals])  # append all where's
                    [values.append(clause[1]) for clause in where_equals]
                # execute
                try:
                    # execute with where params if present
                    cur.execute(f"{sql};", values)
                    conn.commit()
                except connector.Error as oe:
                    # failed to update
                    logging.error(oe)
                    return False
                # print success message if given one
                if cur.rowcount > 0 and on_success:
                    logging.debug(on_success)
                return cur.rowcount > 0  # rows changed

    def _upsert(self, table: Table, primary_key: List[Tuple[str, Any]], updates: List[Tuple[str, Any]],
                print_on_success: bool = True) -> None:
        """
        Generic upsert to the database

        :param table: Table to select from
        :param primary_key: Primary key to update (column, value)
        :param updates: list of updates to the table (column, value)
        :param print_on_success: Print debug message on success (default: True)
        """
        # attempt to update
        msg = None
        if print_on_success:
            msg = ", ".join([f"{pk[0]} '{pk[1]}'" for pk in primary_key])
        if not self._update(table, updates,
                            where_equals=primary_key,
                            on_success=f"Updated {msg}" if print_on_success else None,
                            amend=False):
            # if fail, insert
            updates += primary_key
            self._insert(table, updates,
                         on_success_msg=f"Inserted {msg}" if print_on_success else None)
