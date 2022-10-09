"""
    Allows easy use of sqlite database to for a keyvalue store similar to a
    python dictionary. The values that this database accepts includes any
    value which can be encoded to json via the json.dumps(value) command.
"""

# pylint: disable=consider-using-f-string

import json
import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Set, Tuple

TIMEOUT_OPEN = 60  # Try and fix some of the breakages of sqlite3.


def to_path(sql_uri: str) -> str:
    """Allows sqlite:/// style paths or regular file paths."""
    if sql_uri.startswith("sqlite:///"):
        sql_uri = sql_uri[len("sqlite:///") :]
    return sql_uri


def check_key(key: Optional[str]) -> None:
    """Simple type checking of key."""
    if key is None:
        raise KeyError("Invalid key: %s" % key)
    if not isinstance(key, str):
        raise KeyError("Invalid key type %s" % type(key))


def json_encode(val: Any) -> str:
    """Allows json encoding using utf-8 rather than ascii encoding."""
    return json.dumps(val, sort_keys=True, ensure_ascii=False)


def json_decode(val: str) -> Any:
    """Reflate from objects when retrieving from sqlite."""
    return json.loads(val)


class KeyValueSqlite:
    """
    Acts like a python dictonary but stores values to the backing sqlite
    database file.
    """

    def __init__(
        self, db_path: str, table_name: Optional[str] = None, timeout: int = TIMEOUT_OPEN
    ) -> None:
        """Initialize the database."""
        table_name = table_name or "default"
        self.timeout = timeout
        self.db_path = to_path(db_path)
        folder_path = os.path.dirname(self.db_path)
        os.makedirs(folder_path, exist_ok=True)
        self.table_name = table_name.replace("-", "_")
        if self.db_path in ["", ":memory:"]:
            raise ValueError("Can not use in memory database for keyvalue_db")
        self.create_table()

    def create_table(self) -> None:
        """Creates the table if it doesn't already exist."""
        with self._open_db_for_read() as conn:
            # Check to see if it's exists first of all.
            check_table_stmt = (
                "SELECT name FROM sqlite_master WHERE type='table' AND name='%s';" % self.table_name
            )
            cursor = conn.execute(check_table_stmt)
            has_table = cursor.fetchall()
            if has_table:
                return
        create_stmt = (
            "CREATE TABLE %s (key TEXT PRIMARY KEY UNIQUE NOT NULL, value TEXT);" % self.table_name
        )
        with self._open_db_for_write() as conn:
            try:
                conn.executescript(create_stmt)
            except sqlite3.ProgrammingError:
                pass  # Table already created

    @contextmanager
    def _open_db_for_write(
        self, isolation_level: Optional[str] = None
    ) -> Generator[sqlite3.Connection, None, None]:
        """Obtains an exclusive lock and does a write."""
        isolation_level = isolation_level or "EXCLUSIVE"
        try:
            conn = sqlite3.connect(
                self.db_path,
                isolation_level=isolation_level,
                check_same_thread=False,
                timeout=self.timeout,
            )
        except sqlite3.OperationalError as err:
            raise OSError(f"Error while opening {self.db_path}") from err
        try:
            yield conn
        except:  # noqa: E722
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def _open_db_for_read(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Obtains an exclusive lock and reads. TODO: This should be a shared lock for
        concurrent read access.
        """
        try:
            conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=self.timeout,
            )
        except sqlite3.OperationalError as err:
            raise OSError(f"Error while opening {self.db_path}") from err
        try:
            yield conn
        finally:
            conn.close()

    def set_default(self, key: str, val: Any) -> None:
        """
        Like python dictonary set_default(), only settings the value to the default
        if the value doesn't already exist.
        """
        check_key(key)
        val = json_encode(val)
        insert_stmt = "INSERT OR IGNORE INTO %s (key, value) VALUES (?, ?)" % self.table_name
        record = (key, val)
        with self._open_db_for_write() as conn:
            conn.execute("BEGIN")
            conn.execute(insert_stmt, record)
            conn.commit()

    def __setitem__(self, key: str, item: Any) -> Any:
        """Same as dict.__setitem__()"""
        return self.set(key, item)

    def __getitem__(self, key: str) -> Any:
        """Same as dict.__getitem__()"""
        return self.get_or_raise(key)

    def __missing__(self, key: str) -> None:
        """Same as dict.__missing__()"""
        raise KeyError("Missing key %s" % key)

    def __iter__(self) -> Any:
        """Same as dict.__iter__()"""
        return self.to_dict().__iter__()

    def __contains__(self, key: str) -> bool:
        """Same as dict.__contains__()"""
        return self.has_key(key)  # noqa: W601

    def __delitem__(self, key: str) -> None:
        """Same as dict.__delitem__()"""
        self.remove(key)

    def items(self) -> Any:
        """Same as dict.items()"""
        return self.to_dict().items()

    def __len__(self) -> int:
        """Same as dict.len()"""
        return len(self.items())

    def __repr__(self) -> str:
        """Allows a string representation."""
        out = self.to_dict()
        indent = None
        if len(out) >= 2:
            indent = 4
        return json.dumps(out, sort_keys=True, indent=indent, ensure_ascii=False)

    def __str__(self) -> str:
        """Allows a string representation."""
        return self.__repr__()

    def set(self, key: str, val: Any) -> None:
        """Like dict.set(key) = value"""
        check_key(key)
        val = json_encode(val)
        insert_stmt = "INSERT OR REPLACE INTO %s (key, value) VALUES (?, ?)" % self.table_name
        record = (key, val)
        with self._open_db_for_write() as conn:
            conn.execute("BEGIN")
            conn.execute(insert_stmt, record)
            conn.commit()

    def get(self, key: Optional[str], default: Any = None) -> Any:
        """Like dict.get(key, default)"""
        check_key(key)
        select_stmt = "SELECT value FROM %s WHERE (key = ?)" % self.table_name
        values = (key,)
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(select_stmt, values)
            for row in cursor:
                return json_decode(row[0])
        return default

    def get_or_raise(self, key: str) -> Any:
        """Returns the value if it exists, or throws a KeyError."""
        check_key(key)
        select_stmt = "SELECT value FROM %s WHERE (key = ?)" % self.table_name
        values = (key,)
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(select_stmt, values)
            for row in cursor:
                return json_decode(row[0])
        raise KeyError('Missing key: "%s"' % key)

    def has_key(self, key: str) -> bool:
        """Returns true if the key exists."""
        check_key(key)
        select_stmt = "SELECT value FROM %s WHERE (key = ?)" % self.table_name
        values = (key,)
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(select_stmt, values)
            for _ in cursor:
                return True
        return False

    def keys(self) -> List[str]:
        """Returns a list of keys."""
        select_stmt = "SELECT key FROM %s" % self.table_name
        output = []
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(select_stmt, ())
            for row in cursor:
                output.append(row[0])
        return output

    def key_range(self, key_low: str, key_high: str) -> List[str]:
        """Get keys between key_low and key_high."""
        check_key(key_low)
        check_key(key_high)
        select_stmt = "SELECT key FROM %s WHERE key BETWEEN ? AND ?" % (self.table_name)
        output = []
        values = (key_low, key_high)
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(select_stmt, values)
            for row in cursor:
                output.append(row[0])
        return output

    def get_many(self, a_set: Set[str]) -> Dict[str, Any]:
        """
        Given the set of keys, return a dictionary matching the keys to the
        values.
        """
        select_stmt = "SELECT value FROM %s WHERE (key = ?)" % self.table_name
        output = {}
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            for key in a_set:
                cursor = conn.execute(select_stmt, (key,))
                for row in cursor:
                    output[key] = row[0]
        for key, value in output.items():
            output[key] = json_decode(value)
        return output

    def dict_range(self, key_low: str, key_high: str) -> Dict[str, Any]:
        """
        Returns a dictonary of keys to values.
        """
        check_key(key_low)
        check_key(key_high)
        select_stmt = "SELECT key, value FROM %s WHERE key BETWEEN ? AND ?" % (self.table_name)
        output = []
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(select_stmt, (key_low, key_high))
            for row in cursor:
                entry = (row[0], row[1])
                output.append(entry)
        # Database is closed now.
        out_dict = {}
        for key, val in output:
            out_dict[key] = json_decode(val)
        return out_dict

    def get_range(self, key_low: str, key_high: str) -> List[Tuple[str, Any]]:  # type: ignore
        """Outputs an ordered sequence starting form key_low to key_high."""
        output: List[str, Any]  # type: ignore
        output = []  # type: ignore
        select_stmt = f"SELECT key, value FROM {self.table_name} WHERE key BETWEEN ? AND ?"
        values = (key_low, key_high)
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(select_stmt, values)
            for row in cursor:
                output.append(row[0:2])
        for i, item in enumerate(output):
            output[i] = (item[0], json_decode(item[1]))
        return output

    def remove(self, key: str, ignore_missing_key=False) -> None:  # type: ignore
        """Removes they key, if it exists."""
        check_key(key)
        delete_stmt = "DELETE FROM %s WHERE key=?" % self.table_name
        values = (key,)
        with self._open_db_for_write() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(delete_stmt, values)
            conn.commit()
            if cursor.rowcount == 1:
                return
        if not ignore_missing_key:
            raise KeyError(key)

    def clear(self) -> None:
        """Removes everything from this database."""
        select_stmt = "DELETE FROM %s " % self.table_name
        with self._open_db_for_write() as conn:
            conn.execute("BEGIN")
            _ = conn.execute(select_stmt)
            conn.commit()

    def update(self, a_dict: Dict[str, Any]) -> None:  # type: ignore
        """Like dict.update()"""
        for key in a_dict:
            check_key(key)
        insert_stmt = "INSERT OR REPLACE INTO %s (key, value) VALUES (?, ?)" % self.table_name
        records = [(key, json_encode(val)) for key, val in a_dict.items()]
        with self._open_db_for_write() as conn:
            conn.execute("BEGIN")
            conn.executemany(insert_stmt, records)
            conn.commit()

    def to_dict(self) -> Dict:
        """Returns the whole database as a dictonary of key->value."""
        out = {}
        select_stmt = "SELECT * FROM %s" % self.table_name
        with self._open_db_for_read() as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(select_stmt)
            for row in cursor:
                out[row[0]] = json_decode(row[1])
        return out

    def insert_or_ignore(self, a_dict) -> None:  # type: ignore
        """
        The value is either inserted if missing, or if present, no change
        takes place.
        """
        for key in a_dict:
            check_key(key)
        insert_stmt = "INSERT OR IGNORE INTO %s (key, value) VALUES (?, ?)" % self.table_name
        data = a_dict.items()
        records = [(key, json_encode(val)) for key, val in data]
        with self._open_db_for_write() as conn:
            conn.execute("BEGIN")
            conn.executemany(insert_stmt, records)
            conn.commit()

    def atomic_add(self, key: str, value: int) -> None:
        """
        Adds value to the value associated with key.
        """
        check_key(key)
        update_stmt = f"UPDATE {self.table_name} SET value = value + ? WHERE key = ?"
        insert_stmt = "INSERT INTO %s (key, value) VALUES (?, ?)" % self.table_name
        values = (value, key)
        with self._open_db_for_write(isolation_level="EXCLUSIVE") as conn:
            conn.execute("BEGIN")
            cursor = conn.execute(update_stmt, values)
            if cursor.rowcount == 0:
                conn.execute(insert_stmt, (key, value))
            conn.commit()
