# pydb

> One database interface to rule them all. Wrapper for various python database libraries to simplify use

**🏗️ This is very much a WIP, things are subject to change**

See each respective branch for the database interface usage. As the interfaces mature, they will gradually get merged
into main

## Quickstart

```bash
pip install git+https://github.com/dlg1206/pydb.git@mysql
```

or add to `requirements.txt`

```
pydb @ git+https://github.com/dlg1206/pydb.git@mysql
```

## Databases

### MySQL

Make sure to have a directory with all DDL `.sql` files need to create the database if making a new instance

1. Create an `.env` file
   Creating an `.env` file in this directory and copy the values below. Set any `<...-here>` with your credentials:

```
# connection details
MYSQL_HOST=<db-domain-here>
MYSQL_DATABASE=<db-name-here>
MYSQL_PORT=<db-port-here>
# user details
MYSQL_ROOT_PASSWORD=<root-pw-here>
MYSQL_USER=<username-here>
MYSQL_PASSWORD=<user-pw-here>
```

If hosting locally, set `MYSQL_HOST` to `localhost` and `MYSQL_PORT` to `3306` (default MySQL port)

```python
from sqlalchemy import text

from pydb.common.entity import Table
from pydb.mysql import MySQLDatabase, DEFAULT_POOL_SIZE


# Create table enums matching your ddl
class MyTable(Table):
    TABLE_A = "table_a_name"
    TABLE_B = "table_b_name"
    ...
    TABLE_N = "table_n_name"


# Use generic interface
my_database = MySQLDatabase()
apricons = my_database.select(MyTable.TABLE_N)  # SELECT * FROM table_n_name


# Create your custom interface here
class MyDatabase(MySQLDatabase):

    # call super to create instance
    def __init__(self, pool_size: int = DEFAULT_POOL_SIZE):
        super().__init__(pool_size)

    # example method using prebuilt CRUD
    def count_apricorns(self, color: str) -> int:
        return len(self.select(MyTable.TABLE_N, where_equals={'color': color}))

    # example method using custom sql
    def remove_all_apricons(self, color: str) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                text(f"DELETE FROM `{MyTable.TABLE_N.value}` WHERE color = :color"),
                {"color": color}
            )
```