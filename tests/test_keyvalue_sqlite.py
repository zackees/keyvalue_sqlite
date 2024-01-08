"""
    Test for the keyvalue_sqlite module.
"""


import os
import tempfile
import threading
import unittest

from keyvalue_sqlite.keyvalue_sqlite import KeyValueSqlite


class KeyValueSqliteTester(unittest.TestCase):
    """Tester for the KeyValueSqlite class."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleanup = []

    def tearDown(self):
        for cleanup in self.cleanup:
            try:
                cleanup()
            except BaseException as err:  # pylint: disable = broad-except
                print(str(err))
        self.cleanup = []

    def create_tempfile_path(self) -> str:
        """Create a tempfile path."""
        tmp_file = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        tmp_file.close()
        self.cleanup.append(lambda: os.remove(tmp_file.name))
        return os.path.abspath(tmp_file.name)

    def test_init(self):
        """Test the KeyValueSqlite constructor."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")  # pylint: disable = unused-variable

    def test_set_default(self):
        """Test the set_default method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        db.set_default("0", "1")
        actual_value = db.get("0")
        self.assertEqual("1", actual_value)
        db.set_default("0", "2")
        self.assertEqual("1", db.get("0"))

    def test_insert_or_ignore(self):
        """Test the insert_or_ignore method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        input = {"a": "0", "b": "0"}
        db.insert_or_ignore(input)
        expected_data = {
            "a": "0",
            "b": "0",
        }
        self.assertEqual(expected_data, db.to_dict())
        db.insert_or_ignore({"a": "1", "c": "1"})
        self.assertEqual(db.get("a"), "0")
        self.assertEqual(db.get("c"), "1")

    def test_set(self):
        """Test the set method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        db.set("0", "1")
        self.assertEqual("1", db.get("0"))
        db.set("0", "2")
        self.assertEqual("2", db.get("0"))
        db["4"] = "5"
        try:
            v = db["BAD_KEY"]  # pylint: disable = unused-variable
            self.fail()
        except KeyError as kerr:
            self.assertIn('Missing key: "BAD_KEY"', str(kerr))

    def test_iter(self):
        """Test the iter method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        db.set("0", "1")
        iterated = False
        for key in db:
            iterated = True
            self.assertEqual("0", key)
        self.assertTrue(iterated)
        iterated = False
        for key, val in db.items():
            iterated = True
            self.assertEqual("0", key)
            self.assertEqual("1", val)
        self.assertTrue(iterated)

    def test_default_table_name(self):
        """Test the default table name."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path)
        db.set("0", "1")
        self.assertEqual("1", db.get("0"))
        db.set("0", "2")
        self.assertEqual("2", db.get("0"))
        db["4"] = "5"
        try:
            v = db["BAD_KEY"]  # pylint: disable = unused-variable
            self.fail()
        except KeyError as kerr:
            self.assertIn('Missing key: "BAD_KEY"', str(kerr))

    def test_set_null_key(self):
        """Test the set method with a null key."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        try:
            db.get(None, "1")
            self.assertTrue(False)
        except KeyError:
            pass

    def test_get_not_exist(self):
        """Test the get method with a key that does not exist."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        self.assertIsNone(db.get("0"))
        self.assertEqual("1", db.get("0", "1"))
        self.assertIsNone(db.get("0"))
        db["0"] = "1"
        self.assertEqual(db["0"], "1")

    def test_retrieve(self):
        """Test the retrieve method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        db["a"] = "0"
        db["b"] = "1"
        values = db.get_many({"a", "b"})
        self.assertEqual(values["a"], "0")
        self.assertEqual(values["b"], "1")

    def test_has(self):
        """Test the has method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        self.assertFalse(db.has_key("0"))
        db.set("0", "1")
        self.assertTrue(db.has_key("0"))
        db.remove("0")
        self.assertIsNone(db.get("0"))

    def test_has_key(self):
        """Test the has_key method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        self.assertFalse(db.has_key("0"))
        db.set("0", "1")
        self.assertTrue(db.has_key("0"))
        self.assertIn("0", db)
        db.remove("0")
        self.assertIsNone(db.get("0"))
        self.assertNotIn("0", db)

    def test_keys(self):
        """Test the keys method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        db["a"] = "1"
        db["b"] = "2"
        keys = db.keys()
        self.assertEqual(2, len(keys))
        self.assertTrue("a" in keys)
        self.assertTrue("b" in keys)
        self.assertFalse("c" in keys)

    def test_key_range(self):
        """Test the key_range method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        db["a"] = "1"
        db["b"] = "2"
        db["c"] = "3"
        keys = db.key_range("a", "b")
        self.assertEqual(2, len(keys))
        self.assertTrue("a" in keys)
        self.assertTrue("b" in keys)
        self.assertFalse("c" in keys)
        self.assertEqual([], db.key_range("0", "1"))

    def test_dict_range(self):
        """Test the dict_range method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        db["a"] = "1"
        db["b"] = "2"
        dict_range = db.dict_range("a", "b")
        self.assertEqual({"a": "1", "b": "2"}, dict_range)

    def test_remove(self):
        """Test the remove method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        try:
            db.remove("1")
            self.assertTrue(False)
        except KeyError:
            pass
        db.set("1", "value")
        db.remove("1")
        try:
            db.remove("1")
            self.assertTrue(False)
        except KeyError:
            pass
        self.assertIsNone(db.get("1"))

    def test_update(self):
        """Test the update method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        db.set("0", "1")
        db.set("4", "3")  # Will be overriten
        db.update({"2": "3", "4": "5"})
        expected_data = {"0": "1", "2": "3", "4": "5"}
        self.assertEqual(expected_data, db.to_dict())

    def test_complex_types(self):
        """Test the complex types."""
        db = KeyValueSqlite(self.create_tempfile_path(), "table-name")
        db["0"] = {"key_string": "string", "key_int": 1, "key_float": 2.0}
        out_dict = db.get("0")
        self.assertEqual("string", out_dict["key_string"])
        self.assertEqual(1, out_dict["key_int"])
        self.assertEqual(2.0, out_dict["key_float"])

    def test_clear(self):
        """Test the clear method."""
        db = KeyValueSqlite(self.create_tempfile_path(), "table-name")
        db["0"] = "a"
        self.assertEqual(1, len(db))
        db.clear()
        self.assertEqual(0, len(db))

    def test_to_string(self):
        """Test the to_string method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        self.assertEqual("{}", str(db))
        db.set("0", "1")
        self.assertEqual('{"0": "1"}', str(db))

    def test_atomic_add(self):
        """Test the atomic_add method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        self.assertEqual("{}", str(db))
        db.set("atomic", 1)
        db.atomic_add("atomic", 1)
        self.assertEqual(db.get("atomic"), 2)
        db.atomic_add("atomic", -2)
        self.assertEqual(db.get("atomic"), 0)

    def test_atomic_add_threaded(self):
        """Test the atomic_add method."""
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, "table-name")
        self.assertEqual("{}", str(db))
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

        def runner():
            for value in values:
                db.atomic_add("atomic", value)

        for _ in range(100):
            db.clear()
            t1 = threading.Thread(target=runner)
            t2 = threading.Thread(target=runner)
            t1.start()
            t2.start()
            t1.join()
            t2.join()
            total_value = sum(values) * 2
            self.assertEqual(db.get("atomic"), total_value)


if __name__ == "__main__":
    unittest.main()
