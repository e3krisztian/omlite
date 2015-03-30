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

    PK_NAME = 'id'
    DB_ATTRS = '__db_attrs'

    def __new__(meta, name, bases, attrs):
        cls = type.__new__(meta, name, bases, attrs)
        # set __db_attrs to be all of the Field()-s
        db_attrs = [meta.PK_NAME]

        for base_cls in bases:
            db_attrs.extend(
                attr
                for attr in getattr(base_cls, meta.DB_ATTRS, ())
                if attr not in db_attrs)

        db_attrs.extend(
            attr
            for attr, attr_value in attrs.items()
            if attr not in db_attrs and isinstance(attr_value, Field))

        setattr(cls, meta.DB_ATTRS, db_attrs)
        return cls


class Mapper(object):

    def __init__(self):
        self.object = None
        self.db_attrs = ()
        self.modified_db_attrs = set()

    def connect(self, object):
        self.object = object
        self.db_attrs = getattr(object, ModelMeta.DB_ATTRS)
        # initialize attributes
        for attr in self.db_attrs:
            setattr(object, attr, None)
        self.modified_db_attrs.clear()

    def managed_attr_changed(self, attr):
        if attr in self.db_attrs:
            self.modified_db_attrs.add(attr)

    def save(self):
        if self.object.id is None:
            self.create()
        elif self.modified_db_attrs:
            self.update()

    def create(self):
        self.before_create()
        sql = 'INSERT INTO {}({}) VALUES ({})'.format(
            self.object.get_sqlite3_table_name(),
            ', '.join(self.modified_db_attrs),
            ', '.join(['?'] * len(self.modified_db_attrs))
        )
        values = [getattr(self.object, attr) for attr in self.modified_db_attrs]
        with get_cursor(sql, values) as cursor:
            self.after_create(cursor)
        self.modified_db_attrs.clear()

    def before_create(self):
        pass

    def after_create(self, cursor):
        self.object.id = cursor.lastrowid

    def update(self):
        sql = 'UPDATE {} SET {} WHERE id=?'.format(
            self.object.get_sqlite3_table_name(),
            ', '.join(
                '{} = ?'.format(attr) for attr in self.modified_db_attrs))
        values = [getattr(self.object, attr) for attr in self.modified_db_attrs]
        values += [self.object.id]
        execute_sql(sql, values)
        self.modified_db_attrs.clear()

    def delete(self):
        sql = 'DELETE FROM {} WHERE id=?'.format(
            self.object.get_sqlite3_table_name())
        execute_sql(sql, [self.object.id])
        self.object.id = None


class BaseModel(object):

    def __init__(self, mapper):
        self.__object_mapper = mapper
        self.__object_mapper.connect(self)

    def __setattr__(self, name, value):
        super(BaseModel, self).__setattr__(name, value)
        self.__object_mapper.managed_attr_changed(name)

    @classmethod
    def get_sqlite3_table_name(cls):
        return getattr(cls, 'sqlite3_table_name', cls.__name__.lower())

    # CRUD
    # Create
    # Update
    def save(self):
        self.__object_mapper.save()

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
        self.__object_mapper.delete()


class Model(BaseModel):

    __metaclass__ = ModelMeta

    # primary key
    id = Field('INTEGER PRIMARY KEY')

    def __init__(self):
        super(Model, self).__init__(Mapper())


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
