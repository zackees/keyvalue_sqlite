import json
import os
import sys
import tempfile
import unittest

from keyvalue_sqlite.keyvalue_sqlite import KeyValueSqlite


class KeyValueSqliteTester(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cleanup = []

    def tearDown(self):
        for cleanup in self.cleanup:
            try:
                cleanup()
            except BaseException as be:
                print(str(be))
        self.cleanup = []

    def create_tempfile_path(self) -> str:
        tmp_file = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        tmp_file.close()
        self.cleanup.append(lambda: os.remove(tmp_file.name))
        return os.path.abspath(tmp_file.name)

    def test_KeyValueSqlite_init(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(
            db_path, 'table-name')  # pylint: disable = unused-variable

    def test_set_default(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        db.set_default('0', '1')
        actual_value = db.get('0')
        self.assertEqual('1', actual_value)
        db.set_default('0', '2')
        self.assertEqual('1', db.get('0'))

    def test_insert_or_ignore(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        input = {'a': '0', 'b': '0'}
        db.insert_or_ignore(input)
        expected_data = {
            'a': '0',
            'b': '0',
        }
        self.assertEqual(expected_data, db.to_dict())
        db.insert_or_ignore({'a': '1', 'c': '1'})
        self.assertEqual(db.get('a'), '0')
        self.assertEqual(db.get('c'), '1')

    def test_set(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        db.set('0', '1')
        self.assertEqual('1', db.get('0'))
        db.set('0', '2')
        self.assertEqual('2', db.get('0'))
        db['4'] = '5'
        try:
            v = db['BAD_KEY']  # pylint: disable = unused-variable
            self.assertTrue(False)
        except KeyError as kerr:
            self.assertIn('Missing key: "BAD_KEY"', str(kerr))

    def test_iter(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        db.set('0', '1')
        iterated = False
        for key in db:
            iterated = True
            self.assertEqual('0', key)
        self.assertTrue(iterated)
        iterated = False
        for key, val in db.items():
            iterated = True
            self.assertEqual('0', key)
            self.assertEqual('1', val)
        self.assertTrue(iterated)

    def test_set_null_key(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        try:
            db.get(None, '1')
            self.assertTrue(False)
        except KeyError:
            pass

    def test_get_not_exist(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        self.assertIsNone(db.get('0'))
        self.assertEqual('1', db.get('0', '1'))
        self.assertIsNone(db.get('0'))
        db['0'] = '1'
        self.assertEqual(db['0'], '1')

    def test_retrieve(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        db['a'] = '0'
        db['b'] = '1'
        values = db.get_many({'a', 'b'})
        self.assertEqual(values['a'], '0')
        self.assertEqual(values['b'], '1')

    def test_has(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        self.assertFalse(db.has_key('0'))
        db.set('0', '1')
        self.assertTrue(db.has_key('0'))
        db.remove('0')
        self.assertIsNone(db.get('0'))

    def test_has_key(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        self.assertFalse(db.has_key('0'))
        db.set('0', '1')
        self.assertTrue(db.has_key('0'))
        self.assertIn('0', db)
        db.remove('0')
        self.assertIsNone(db.get('0'))
        self.assertNotIn('0', db)

    def test_keys(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        db['a'] = '1'
        db['b'] = '2'
        keys = db.keys()
        self.assertEqual(2, len(keys))
        self.assertTrue('a' in keys)
        self.assertTrue('b' in keys)
        self.assertFalse('c' in keys)

    def test_key_range(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        db['a'] = '1'
        db['b'] = '2'
        db['c'] = '3'
        keys = db.key_range('a', 'b')
        self.assertEqual(2, len(keys))
        self.assertTrue('a' in keys)
        self.assertTrue('b' in keys)
        self.assertFalse('c' in keys)
        self.assertEqual([], db.key_range('0', '1'))

    def test_dict_range(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        db['a'] = '1'
        db['b'] = '2'
        dict_range = db.dict_range('a', 'b')
        self.assertEqual({'a': '1', 'b': '2'}, dict_range)

    def test_remove(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        try:
            db.remove('1')
            self.assertTrue(False)
        except KeyError:
            pass
        db.set('1', 'value')
        db.remove('1')
        try:
            db.remove('1')
            self.assertTrue(False)
        except KeyError:
            pass
        self.assertIsNone(db.get('1'))

    def test_update(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        db.set('0', '1')
        db.set('4', '3')  # Will be overriten
        db.update({'2': '3', '4': '5'})
        expected_data = {
            '0': '1',
            '2': '3',
            '4': '5'
        }
        self.assertEqual(expected_data, db.to_dict())

    def test_complex_types(self):
        db = KeyValueSqlite(self.create_tempfile_path(), 'table-name')
        db['0'] = {'key_string': 'string', 'key_int': 1, 'key_float': 2.0}
        out_dict = db.get('0')
        self.assertEqual('string', out_dict['key_string'])
        self.assertEqual(1, out_dict['key_int'])
        self.assertEqual(2.0, out_dict['key_float'])

    def test_clear(self):
        db = KeyValueSqlite(self.create_tempfile_path(), 'table-name')
        db['0'] = 'a'
        self.assertEqual(1, len(db))
        db.clear()
        self.assertEqual(0, len(db))

    def test_to_string(self):
        db_path = self.create_tempfile_path()
        db = KeyValueSqlite(db_path, 'table-name')
        self.assertEqual('{}', str(db))
        db.set('0', '1')
        self.assertEqual('{"0": "1"}', str(db))


if __name__ == '__main__':
    unittest.main()
