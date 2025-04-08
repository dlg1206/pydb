# pydb

> One database interface to rule them all. This is a wrapper for various python database libraries to simplify use
> between them

**🏗️ This is very much a WIP, things are subject to change**

See each respective branch for the database interface usage. As the interfaces mature, they will gradually get merged
into main

## Quickstart

```bash
pip install git+https://github.com/dlg1206/pydb.git@sqlite
```

or add to `requirements.txt`
```
pydb @ git+https://github.com/dlg1206/pydb.git@sqlite
```

## Databases

### SQLite

Make sure to have a directory with all DDL `.sql` files need to create the database if making a new instance

```python
from pydb.common.entity import Table
from pydb.sqlite import SQLiteDatabase


# Create table enums matching your ddl
class MyTable(Table):
    TABLE_A = "table_a_name"
    TABLE_B = "table_b_name"
    ...
    TABLE_N = "table_n_name"


# Use generic interface
my_database = SQLiteDatabase("path/to/ddl")
apricons = my_database.select(MyTable.TABLE_N)  # SELECT * FROM table_n_name


# Create your custom interface here
class MyDatabase(SQLiteDatabase):

    # call super to create instance
    def __init__(self, db_location: str, ddl_location: str = None, force_rebuild: bool = False):
        super().__init__(db_location, ddl_location, force_rebuild)

    # example method using prebuilt CRUD
    def count_apricorns(self, color: str) -> int:
        return len(self.select(MyTable.TABLE_N, where_equals=[('color', color)]))

    # example method using custom sql
    def remove_all_apricons(self, color: str) -> None:
        with self.open_connection() as conn:
            with self.get_cursor(conn) as cur:
                cur.execute("DELETE FROM table_n_name WHERE color = ?;", (color,))
                conn.commit()
```
