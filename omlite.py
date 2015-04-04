'''
O      M            LITE
 bject  apper for SQ     - an experiment

the R from ORM is intentionally missing, R is support for relations.

Restrictions by design:

- maps class to table, table row to object isinstances
- relations are *NOT* supported, only single objects
- the name of the primary key is *id*.
- no query language - SQL has one already

TODO?: make(storable_class, **field_values)
TODO: create(object)
TODO: create_table(storable_class)
'''

import contextlib
import functools
import sqlite3
import uuid


__all__ = (
    'Database',
    'storable_pk_autoinc',
    'storable_pk_netaddrtime_uuid1', 'storable_pk_random_uuid4',
    'Field',
    # for more control:
    'database', 'table_name',
    'get_storable',
    'PrimaryKey', 'UUIDPrimaryKey', 'AutoincrementPrimaryKey',
)


AUTOCOMMIT = None
PK_FIELD = 'id'
STORABLE_META_ATTR = '__omlite_meta'


class Database(object):

    connection = None

    def __init__(self, dbref=':memory:'):
        self.connection = None
        self.open_transactions = 0
        if dbref:
            self.connect(dbref)

    def connect(self, dbref):
        '''
        in sqlite3 dbref is either ':memory:' or a filename
        '''
        self.connection = sqlite3.connect(dbref)
        self.connection.isolation_level = AUTOCOMMIT
        self.enable_foreign_keys()

    # Administration
    def pragma_foreign_keys(self, extra=''):
        return self.connection.execute(
            'PRAGMA foreign_keys{}'.format(extra)
        ).fetchone()

    def enable_foreign_keys(self):
        self.pragma_foreign_keys(extra='=ON')

    def disable_foreign_keys(self):
        self.pragma_foreign_keys(extra='=OFF')

    def get_cursor(self, sql, params):
        '''
        with get_cursor('INSERT ... ?', ['1', ...]) as c:
            # work with cursor c
        '''
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, params)
            return contextlib.closing(cursor)
        except:
            cursor.close()
            raise

    def execute_sql(self, sql, params):
        with self.get_cursor(sql, params):
            pass

    # Transactions
    @contextlib.contextmanager
    def transaction(self):
        assert self.open_transactions >= 0

        # transactions work only when the connection is in autocommit mode
        # https://pysqlite.readthedocs.org/en/latest/sqlite3.html#controlling-transactions
        # https://github.com/ghaering/pysqlite/issues/24
        # https://groups.google.com/forum/#!msg/sqlalchemy-devel/0lanNjxSpb0/6zriniGAfu0J
        # http://bugs.python.org/issue10740
        # http://rogerbinns.github.io/apsw/pysqlite.html#pysqlitediffs
        assert self.connection.isolation_level is AUTOCOMMIT

        execute = self.connection.execute
        savepoint_name = 'omlite_{}'.format(self.open_transactions)
        execute('SAVEPOINT {}'.format(savepoint_name))
        try:
            self.open_transactions += 1
            yield
            execute('RELEASE SAVEPOINT {}'.format(savepoint_name))
        except:
            execute('ROLLBACK TO SAVEPOINT {}'.format(savepoint_name))
            raise
        finally:
            self.open_transactions -= 1

'''
Single global instance - when only one database is needed
'''
db = Database()


class Field(object):

    def __init__(self, sql_declaration=None):
        self.sql_declaration = sql_declaration


class PrimaryKey(Field):

    def generate_id(self, object):
        pass

    def save_generated_id(self, cursor, object):
        if object.id is None:
            object.id = cursor.lastrowid


class AutoincrementPrimaryKey(PrimaryKey):

    def __init__(self):
        super(AutoincrementPrimaryKey, self).__init__('INTEGER PRIMARY KEY')


class UUIDPrimaryKey(PrimaryKey):

    def __init__(self, uuid_generator):
        super(UUIDPrimaryKey, self).__init__('VARCHAR PRIMARY KEY')
        self.uuid_generator = uuid_generator

    def generate_id(self, object):
        if object.id is None:
            object.id = str(self.uuid_generator())


def get_db_fields(cls):
    fields = {}

    for attr in dir(cls):
        field = getattr(cls, attr)
        if isinstance(field, Field):
            fields[attr] = field

    assert isinstance(fields[PK_FIELD], PrimaryKey)
    return fields


class StorableMeta(object):

    def __init__(self, storable_class):
        self.fields = get_db_fields(storable_class)
        self.ordered_fields = tuple(sorted(self.fields))
        self.primary_key = self.fields[PK_FIELD]
        self.database = db
        self.table_name = storable_class.__name__.lower()

    def initialize_fields(self, object):
        ''' initialize all uninitialized database fields to None'''
        for attr, field in self.fields.items():
            if getattr(object, attr) is field:
                setattr(object, attr, None)


# Class decorators
def database(database):
    ''' Set database on a storable class

    @database(dbx)
    @storable
    class Data(object):
        ...
    '''
    def decorate(storable_class):
        meta = get_class_meta(storable_class)
        assert meta is not None
        meta.database = database
        return storable_class
    return decorate


def table_name(table_name):
    ''' Set table_name on a storable class

    @table_name('special_name')
    @storable
    class Data(object):
        ...
    '''
    def decorate(storable_class):
        meta = get_class_meta(storable_class)
        assert meta is not None
        meta.table_name = table_name
        return storable_class
    return decorate


def get_storable(cls, id):
    setattr(cls, PK_FIELD, id)
    assert PK_FIELD in dir(cls)
    meta = StorableMeta(cls)

    class decorated(cls):
        def __init__(self, *args, **kwargs):
            super(decorated, self).__init__(*args, **kwargs)
            meta.initialize_fields(self)
    decorated.__name__ = 'omlite_{}'.format(cls.__name__)
    setattr(decorated, STORABLE_META_ATTR, meta)
    return decorated

storable_pk_autoinc = functools.partial(
    get_storable,
    id=AutoincrementPrimaryKey())

storable_pk_netaddrtime_uuid1 = functools.partial(
    get_storable,
    id=UUIDPrimaryKey(uuid.uuid1))

storable_pk_random_uuid4 = functools.partial(
    get_storable,
    id=UUIDPrimaryKey(uuid.uuid4))


# Internals
def get_class_meta(storable_class):
    return getattr(storable_class, STORABLE_META_ATTR)


def get_meta(object):
    return get_class_meta(object.__class__)


def read_row(storable_class, cursor):
    meta = get_class_meta(storable_class)

    row = next(cursor)
    obj = storable_class()
    for idx, col in enumerate(cursor.description):
        dbattr = col[0]
        assert dbattr in meta.fields
        # TODO: convert/validate value as specified by Field
        setattr(obj, dbattr, row[idx])

    meta.initialize_fields(obj)
    return obj


# CRUD / Object Mapper
def get(storable_class, id):
    return list(filter(storable_class, 'id=?', id))[0]


def filter(storable_class, sql_predicate, *params):
    meta = get_class_meta(storable_class)

    sql = 'SELECT * FROM {table} WHERE {predicate}'.format(
        table=meta.table_name, predicate=sql_predicate)

    with meta.database.get_cursor(sql, params) as cursor:
        while True:
            yield read_row(storable_class, cursor)


def save(object):
    if object.id is None:
        create(object)
    else:
        _update(object)


def create(object):
    meta = get_meta(object)

    meta.primary_key.generate_id(object)

    sql = 'INSERT INTO {table}({fields}) VALUES ({values})'.format(
        table=meta.table_name,
        fields=', '.join(meta.ordered_fields),
        values=', '.join(['?'] * len(meta.ordered_fields)))

    values = [getattr(object, attr) for attr in meta.ordered_fields]
    with meta.database.get_cursor(sql, values) as cursor:
        meta.primary_key.save_generated_id(cursor, object)


def _update(object):
    meta = get_meta(object)
    fields = ['{} = ?'.format(attr) for attr in meta.ordered_fields]
    values = [getattr(object, attr) for attr in meta.ordered_fields]
    pk_value = object.id

    sql = 'UPDATE {table} SET {fields} WHERE id=?'.format(
        table=meta.table_name,
        fields=', '.join(fields))

    meta.database.execute_sql(sql, values + [pk_value])


def delete(object):
    meta = get_meta(object)

    sql = 'DELETE FROM {table} WHERE id=?'.format(table=meta.table_name)
    meta.database.execute_sql(sql, [object.id])

    object.id = None
