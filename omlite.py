
'''
Object Mapper for sqLITE

Support for mapping relations between objects is intentionally missing.

Goals:

- one to one mapping between objects and database rows
- correctness
- ease of use
- small, simple implementation

Restrictions by design:

- maps class to table, table row to object instances
- relations between objects at the Python level are *NOT* supported
    + at the database level constraints including foreign keys are supported
- the name of the primary key is *id*.

Query language is SQL based.

TODO?: make(storable_class, **field_values)
TODO: logging
'''

import contextlib
import functools
import sqlite3
import uuid


__all__ = (
    'db',
    'storable_pk_autoinc',
    'storable_pk_netaddrtime_uuid1', 'storable_pk_random_uuid4',
    'Field',
    # CRUD / Data Mapper functions
    'get', 'filter', 'save', 'create', 'delete', 'delete_but_keep_id',
    # for more control and extras
    'Database', 'database', 'table_name', 'sql_constraint',
    'table_exists', 'create_table',
    'get_storable',
    'PrimaryKey', 'UUIDPrimaryKey', 'AutoincrementPrimaryKey',
    'IntegrityError',
)


AUTOCOMMIT = None
PK_FIELD = 'id'
STORABLE_META_ATTR = '__omlite_meta'
IntegrityError = sqlite3.IntegrityError


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
        self.table_name = '{}s'.format(storable_class.__name__.lower())
        self.constraints = []

    def initialize_fields(self, object):
        ''' initialize all uninitialized database fields to None'''
        for attr, field in self.fields.items():
            if getattr(object, attr) is field:
                setattr(object, attr, None)

    def add_constraint(self, constraint):
        self.constraints.append(constraint)


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


def sql_constraint(contstraint):
    ''' Add a table constraint to a storable class definition

    Makes a difference only when the database table is generated
    by create_table().
    '''
    def decorate(storable_class):
        meta = get_class_meta(storable_class)
        assert meta is not None
        meta.add_constraint(contstraint)
        return storable_class
    return decorate


def get_storable(cls, id):
    setattr(cls, PK_FIELD, id)
    assert PK_FIELD in dir(cls)

    meta = StorableMeta(cls)

    # patch class to initialize its fields

    # originally cls was subclassed and initialization happened in the new
    # class, however it resulted in an endless recursion under python2:
    # where
    #   super(ClassName, self).__init__()
    # equals
    #   ClassName.__init__(self)
    # if ClassName refers to the subclass (which it does after @storable
    # decoration)
    orig_init = cls.__init__
    def __init__(self, *args, **kwargs):
        original_return_value = orig_init(self, *args, **kwargs)
        meta.initialize_fields(self)
        return original_return_value
    __init__.__doc__ = orig_init.__doc__
    cls.__init__ = __init__

    setattr(cls, STORABLE_META_ATTR, meta)

    return cls

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
    ''' I retrieve an object from database by its :id.

    raise LookupError if no row was found.
    '''
    return list(filter(storable_class, 'id=?', id))[0]


def filter(storable_class, sql_predicate, *params):
    ''' I am streaming objects from database that match the predicate.
    '''
    meta = get_class_meta(storable_class)

    sql = 'SELECT * FROM {table} WHERE {predicate}'.format(
        table=meta.table_name, predicate=sql_predicate)

    with meta.database.get_cursor(sql, params) as cursor:
        while True:
            yield read_row(storable_class, cursor)


def get_all(storable_class):
    ''' I am streaming all objects in the database.
    '''
    # see https://www.sqlite.org/datatype3.html
    true = 1
    return filter(storable_class, sql_predicate=true)


def save(object):
    ''' I create new or update existing object in the database.

    An object is treated as new, if its :id is None.
    If the object's id is not None, the matching row is updated.
    '''
    if object.id is None:
        create(object)
    else:
        _update(object)


def create(object):
    ''' I create new object in the database.

    There are two cases:
    - the object has None in the id field
      - a generated id is assigned to :object.id
    - the object has the id field pre-filled
      - object is inserted into database with the given id
    '''
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
    set_fields = ['{} = ?'.format(attr) for attr in meta.ordered_fields]
    values = [getattr(object, attr) for attr in meta.ordered_fields]

    sql = 'UPDATE {table} SET {set_fields} WHERE id=?'.format(
        table=meta.table_name,
        set_fields=', '.join(set_fields))

    meta.database.execute_sql(sql, values + [object.id])


def delete_but_keep_id(object):
    ''' I delete object from database.
    '''
    meta = get_meta(object)

    sql = 'DELETE FROM {table} WHERE id=?'.format(table=meta.table_name)
    meta.database.execute_sql(sql, [object.id])


def delete(object, clear_id=True):
    ''' I delete object from database.

    I also set the object's :id to None, so it can be resaved if needed.
    '''
    delete_but_keep_id(object)
    object.id = None


# Database structure
def table_exists(storable_class):
    meta = get_class_meta(storable_class)
    sql = '''SELECT 1 FROM sqlite_master where type='table' and name=?'''
    with meta.database.get_cursor(sql, [meta.table_name]) as c:
        return bool(list(c))


def create_table(storable_class):
    meta = get_class_meta(storable_class)

    def define_field(attr, sql_declaration):
        if sql_declaration:
            return '{} {}'.format(attr, sql_declaration)
        return attr

    field_definitions = [
        define_field(attr, field.sql_declaration)
        for attr, field in meta.fields.items()
    ]
    field_sep = ',\n' + (' ' * 8)
    sql = '''
    CREATE TABLE {table_name}(
        {field_definitions}
    );
    '''.format(
        table_name=meta.table_name,
        field_definitions=field_sep.join(field_definitions + meta.constraints),
    )
    meta.database.connection.execute(sql)
