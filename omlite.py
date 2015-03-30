'''
O      M            LITE
 bject  apper for SQ     - an experiment

the R from ORM is intentionally missing, R is support for relations.
'''

import contextlib
import functools
import sqlite3

connection = None


def pragma_foreign_keys(extra=''):
    return connection.execute('PRAGMA foreign_keys{}'.format(extra)).fetchone()

enable_foreign_keys = functools.partial(pragma_foreign_keys, '=ON')
disable_foreign_keys = functools.partial(pragma_foreign_keys, '=OFF')


def connect(db):
    global connection
    connection = sqlite3.connect(db)
    connection.isolation_level = 'EXCLUSIVE'
    enable_foreign_keys()


# TODO: DistributedModel with uuid as primary key
# TODO: create_table(Model)
# TODO: transactions
# FIXME: CRUD operations must be executable only inside transactions!


def get_cursor(sql, params):
    '''
    with get_cursor('INSERT ... ?', ['1', ...]) as c:
        # work with cursor c
    '''
    cursor = connection.cursor()
    try:
        cursor.execute(sql, params)
        return contextlib.closing(cursor)
    except:
        cursor.close()
        raise


def execute_sql(sql, params):
    with get_cursor(sql, params):
        pass


class Field(object):

    def __init__(self, type=None):
        self.type = type


class ModelMeta(type):

    def __new__(meta, name, bases, attrs):
        # set _db_attrs to be all of the Field()-s
        db_attrs = ['id']
        for base_cls in bases:
            db_attrs.extend(
                attr for attr in getattr(base_cls, '_db_attrs', ())
                if attr not in db_attrs)
        db_attrs.extend(
            attr_name
            for attr_name, attr_value in attrs.items()
            if attr_name not in db_attrs and isinstance(attr_value, Field))
        cls = type.__new__(
            meta, name, bases, dict(attrs, _db_attrs=tuple(db_attrs)))
        return cls


class Model(object):

    __metaclass__ = ModelMeta

    # primary key
    id = Field('INTEGER PRIMARY KEY')

    _db_attrs = ()

    def __init__(self):
        self.__modified_db_attrs = set()
        # set all fields to None
        for attr in self._db_attrs:
            setattr(self, attr, None)
        # make all fields clean
        self.__modified_db_attrs.clear()

    def __setattr__(self, name, value):
        super(Model, self).__setattr__(name, value)
        if name in self._db_attrs:
            self.__modified_db_attrs.add(name)

    @classmethod
    def get_sqlite3_table_name(cls):
        return getattr(cls, 'sqlite3_table_name', cls.__name__.lower())

    # CRUD
    # Create
    # Update
    def save(self):
        if self.id is None:
            self._create()
        elif self.__modified_db_attrs:
            self._update()

    def _create(self):
        sql = 'INSERT INTO {}({}) VALUES ({})'.format(
            self.get_sqlite3_table_name(),
            ', '.join(self.__modified_db_attrs),
            ', '.join(['?'] * len(self.__modified_db_attrs))
        )
        values = [getattr(self, attr) for attr in self.__modified_db_attrs]
        with get_cursor(sql, values) as cursor:
            self.id = cursor.lastrowid
        self.__modified_db_attrs.clear()

    def _update(self):
        sql = 'UPDATE {} SET {} WHERE id=?'.format(
            self.get_sqlite3_table_name(),
            ', '.join(
                '{} = ?'.format(attr) for attr in self.__modified_db_attrs))
        values = [getattr(self, attr) for attr in self.__modified_db_attrs]
        values += [self.id]
        execute_sql(sql, values)
        self.__modified_db_attrs.clear()

    # Read
    @classmethod
    def _read(cls, cursor):
        row = next(cursor)
        obj = cls()
        for idx, col in enumerate(cursor.description):
            dbattr = col[0]
            field = getattr(cls, dbattr)
            assert isinstance(field, Field)
            # TODO: convert value as specified by Field
            setattr(obj, dbattr, row[idx])
        # FIXME: mark obj as clean object - no need to save/update
        return obj

    @classmethod
    def select(cls, where, *params):
        sql = 'SELECT * FROM {} WHERE {}'.format(
            cls.get_sqlite3_table_name(), where)
        with get_cursor(sql, params) as cursor:
            while True:
                yield cls._read(cursor)

    @classmethod
    def by_id(cls, id):
        return list(cls.select('id=?', id))[0]

    # Delete
    def delete(self):
        sql = 'DELETE FROM {} WHERE id=?'.format(
            self.get_sqlite3_table_name())
        execute_sql(sql, [self.id])
        self.id = None


##############################################################################
import unittest


class A(Model):
    sqlite3_table_name = 'aa'
    a = Field()


class B(Model):
    b = Field()


class X(A, B):
    x = Field()


def given_a_database():
    connect(':memory:')
    connection.executescript(
        '''\
        create table aa(id integer primary key, a);
        insert into aa(id, a) values (0, 'A() in db at 0');
        insert into aa(id, a) values (1, 'A() in db at 1');
        create table b(id integer primary key, b);
        insert into b(id, b) values (0, 'B() in db at 0');
        insert into b(id, b) values (2, 'B() in db at 2');
        create table x(id integer primary key, a, b, x);
        insert into x(id, b) values (2, 'X() in db at 2');
        ''')


class Test_Model_READ(unittest.TestCase):

    def test_get_sqlite3_table_name(self):
        self.assertEqual('model', Model.get_sqlite3_table_name())
        self.assertEqual('aa', A.get_sqlite3_table_name())
        self.assertEqual('b', B.get_sqlite3_table_name())

    def test_by_id(self):
        given_a_database()

        a = A.by_id(0)

        self.assertEqual(0, a.id)
        self.assertEqual('A() in db at 0', a.a)
        self.assertIsInstance(a, A)

        b = B.by_id(2)

        self.assertEqual(2, b.id)
        self.assertEqual('B() in db at 2', b.b)
        self.assertIsInstance(b, B)


class Test_CREATE(unittest.TestCase):

    def test(self):
        given_a_database()
        a = A()
        a.a = 'A created in db'
        a.save()

        a_from_db = A.by_id(a.id)

        self.assertEqual('A created in db', a_from_db.a)


class Test_UPDATE(unittest.TestCase):

    def test(self):
        given_a_database()
        a = A.by_id(0)
        a.a = 'overwritten field'
        a.save()

        a_from_db = A.by_id(0)
        self.assertEqual('overwritten field', a_from_db.a)
        self.assertNotEqual(id(a), id(a_from_db))


class Test_DELETE(unittest.TestCase):

    def test_deleted(self):
        given_a_database()
        a = A.by_id(0)
        a.delete()

        self.assertIsNone(a.id)
        self.assertRaises(IndexError, A.by_id, 0)

    def test_deleted_can_be_resaved_with_new_id(self):
        given_a_database()
        a = A.by_id(0)
        a.delete()

        a.save()

        a_from_db = A.by_id(a.id)
        self.assertEqual(a.a, a_from_db.a)

if __name__ == '__main__':
    unittest.main()
