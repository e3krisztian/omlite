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

    PK_FIELD = 'id'
    DB_ATTRS = '__db_attrs'

    def __new__(meta, name, bases, attrs):
        cls = type.__new__(meta, name, bases, attrs)
        # set __db_attrs to be all of the Field()-s
        db_attrs = [meta.PK_FIELD]

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

    PK_FIELD = 'id'

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
        self.mark_clean()

    def mark_clean(self):
        self.modified_db_attrs.clear()

    def managed_attr_changed(self, attr):
        if attr in self.db_attrs:
            self.modified_db_attrs.add(attr)

    def save(self):
        if getattr(self.object, self.PK_FIELD) is None:
            self.create()
        elif self.modified_db_attrs:
            self.update()

    def create(self):
        object = self.object
        self.before_create()
        sql = 'INSERT INTO {table}({fields}) VALUES ({values})'.format(
            table=object.get_sqlite3_table_name(),
            fields=', '.join(self.modified_db_attrs),
            values=', '.join(['?'] * len(self.modified_db_attrs)))
        values = [getattr(object, attr) for attr in self.modified_db_attrs]
        with get_cursor(sql, values) as cursor:
            self.after_create(cursor)
        self.mark_clean()

    def before_create(self):
        pass

    def after_create(self, cursor):
        setattr(self.object, self.PK_FIELD, cursor.lastrowid)

    def update(self):
        assert self.PK_FIELD not in self.modified_db_attrs
        object = self.object
        fields = ['{} = ?'.format(attr) for attr in self.modified_db_attrs]
        values = [getattr(object, attr) for attr in self.modified_db_attrs]
        pk_value = getattr(object, self.PK_FIELD)

        sql = 'UPDATE {table} SET {fields} WHERE {id}=?'.format(
            table=object.get_sqlite3_table_name(),
            fields=', '.join(fields),
            id=self.PK_FIELD)

        execute_sql(sql, values + [pk_value])
        self.mark_clean()

    def delete(self):
        object = self.object
        sql = 'DELETE FROM {table} WHERE {id}=?'.format(
            table=object.get_sqlite3_table_name(), id=self.PK_FIELD)
        execute_sql(sql, [getattr(object, self.PK_FIELD)])
        setattr(object, self.PK_FIELD, None)
        # mark all non-pk attributes modified for re-save
        self.modified_db_attrs.update(
            attr
            for attr in self.db_attrs
            if getattr(object, attr) is not None)


def _read_row(row_class, cursor):
    row = next(cursor)
    obj = row_class()
    for idx, col in enumerate(cursor.description):
        dbattr = col[0]
        field = getattr(row_class, dbattr)
        assert isinstance(field, Field)
        # TODO: convert value as specified by Field
        setattr(obj, dbattr, row[idx])
    obj.mark_db_attributes_clean()
    return obj


class BaseModel(object):

    def __init__(self, mapper):
        self.__object_mapper = mapper
        self.__object_mapper.connect(self)

    def __setattr__(self, name, value):
        super(BaseModel, self).__setattr__(name, value)
        self.__object_mapper.managed_attr_changed(name)

    def mark_db_attributes_clean(self):
        self.__object_mapper.mark_clean()

    @classmethod
    def get_sqlite3_table_name(cls):
        return getattr(cls, 'sqlite3_table_name', cls.__name__.lower())

    def save(self):
        self.__object_mapper.save()

    @classmethod
    def select(cls, sql_predicate, *params):
        sql = 'SELECT * FROM {table} WHERE {predicate}'.format(
            table=cls.get_sqlite3_table_name(), predicate=sql_predicate)
        with get_cursor(sql, params) as cursor:
            while True:
                yield _read_row(cls, cursor)

    def delete(self):
        self.__object_mapper.delete()


class Model(BaseModel):

    __metaclass__ = ModelMeta

    # primary key managed by omlite
    id = Field('INTEGER PRIMARY KEY')

    def __init__(self):
        super(Model, self).__init__(Mapper())

    @classmethod
    def by_id(cls, id):
        return list(cls.select('id=?', id))[0]
