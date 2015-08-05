import unittest

from omlite import db, Field
from omlite import storable_pk_autoinc, storable_pk_netaddrtime_uuid1
from omlite import get_class_meta, table_name
from omlite import Database, database
import omlite as m


@table_name('aa')
@storable_pk_autoinc
class A(object):
    a = Field()


@storable_pk_autoinc
class B(object):
    b = Field()


@table_name('x')
@storable_pk_autoinc
class AB(A, B):
    x = Field()


TEST_UUID = '9764d716-2b5b-4a6f-9c75-621377a80028'


def make(cls, **attrs):
    obj = cls()
    for attr, value in attrs.items():
        setattr(obj, attr, value)
    return obj


def insert(cls, **attrs):
    obj = make(cls, **attrs)
    m.save(obj)
    return obj.id


def given_a_database():
    db.connect(':memory:')
    db.connection.executescript(
        '''\
        create table aa(id integer primary key, a);
        insert into aa(id, a) values (0, 'A() in db at 0');
        insert into aa(id, a) values (1, 'A() in db at 1');
        create table bs(id integer primary key, b);
        insert into bs(id, b) values (0, 'B() in db at 0');
        insert into bs(id, b) values (2, 'B() in db at 2');
        create table x(id integer primary key, a, b, x);
        insert into x(id, b) values (2, 'X() in db at 2');
        create table fs(id varchar primary key, future);
        insert into fs(id, future)
            values ('9764d716-2b5b-4a6f-9c75-621377a80028', '?');
        ''')


class TestCase(unittest.TestCase):

    def setUp(self):
        given_a_database()


class Test_meta(unittest.TestCase):

    def test_table_name(self):
        self.assertEqual('aa', get_class_meta(A).table_name)
        self.assertEqual('bs', get_class_meta(B).table_name)


class Test_storable_READ(TestCase):

    def test_by_id(self):
        a = m.get(A, 0)

        self.assertEqual(0, a.id)
        self.assertEqual('A() in db at 0', a.a)
        self.assertIsInstance(a, A)

        b = m.get(B, 2)

        self.assertEqual(2, b.id)
        self.assertEqual('B() in db at 2', b.b)
        self.assertIsInstance(b, B)

    def test_filter(self):
        objects = list(m.filter(A, 'a like "%1"'))
        a, = objects
        self.assertEqual(1, a.id)

    def test_get_all(self):
        objects = sorted(m.get_all(A), key=lambda a: a.a)
        a0, a1 = objects
        self.assertEqual(0, a0.id)
        self.assertEqual(1, a1.id)


class Test_storable_CREATE(TestCase):

    def test(self):
        a = A()
        a.a = 'A created in db'
        m.save(a)

        a_from_db = m.get(A, a.id)

        self.assertEqual('A created in db', a_from_db.a)

    def test_create_with_id(self):
        a = A()
        a.id = 8080
        a.a = 'succeeded'
        m.create(a)

        a_from_db = m.get(A, id=8080)
        self.assertEqual('succeeded', a_from_db.a)

    def test_create_without_id(self):
        a = A()
        a.a = 'succeeded'
        m.create(a)
        self.assertIsNotNone(a.id)

        a_from_db = m.get(A, id=a.id)
        self.assertEqual('succeeded', a_from_db.a)


class Test_storable_UPDATE(TestCase):

    def test(self):
        a = m.get(A, 0)
        a.a = 'overwritten field'
        m.save(a)

        a_from_db = m.get(A, 0)
        self.assertEqual('overwritten field', a_from_db.a)
        self.assertNotEqual(id(a), id(a_from_db))


class Test_storable_DELETE(TestCase):

    def test_deleted(self):
        a = m.get(A, 0)
        m.delete(a)

        self.assertIsNone(a.id)
        self.assertRaises(LookupError, m.get, A, 0)

    def test_deleted_can_be_resaved_with_new_id(self):
        a = m.get(A, 0)
        m.delete(a)

        m.save(a)

        a_from_db = m.get(A, a.id)
        self.assertEqual(a.a, a_from_db.a)


class Test_storable_inheritance(TestCase):

    def test_by_id(self):
        ab = m.get(AB, 2)

        self.assertEqual(ab.b, 'X() in db at 2')

    def test_update(self):
        ab = m.get(AB, 2)
        ab.a = 'persisted attribute'
        m.save(ab)

        ab_from_db = m.get(AB, 2)

        self.assertEqual(ab_from_db.b, 'X() in db at 2')
        self.assertEqual(ab.a, 'persisted attribute')


@storable_pk_netaddrtime_uuid1
class F(object):
    future = Field()


class Test_Storable_with_UUID_primary_key(TestCase):

    def test_create(self):
        id = insert(F, future='newly saved')

        from_db = m.get(F, id)
        self.assertEqual('newly saved', from_db.future)

    def test_create_with_id(self):
        f = F()
        f.id = 'create with uuid'
        f.future = 'succeeded'
        m.create(f)

        f_from_db = m.get(F, id='create with uuid')
        self.assertEqual('succeeded', f_from_db.future)

    def test_create_without_id(self):
        f = F()
        f.future = 'succeeded'
        m.create(f)
        self.assertIsNotNone(f.id)

        f_from_db = m.get(F, id=f.id)
        self.assertEqual('succeeded', f_from_db.future)

    def test_update(self):
        f = m.get(F, TEST_UUID)
        f.future = 'unknown'
        m.save(f)

        from_db = m.get(F, TEST_UUID)
        self.assertEqual('unknown', from_db.future)

    def test_delete(self):
        f = m.get(F, TEST_UUID)
        m.delete(f)

        self.assertIsNone(f.id)
        self.assertRaises(LookupError, m.get, F, TEST_UUID)

    def test_by_id(self):
        from_db = m.get(F, TEST_UUID)
        self.assertEqual('?', from_db.future)


class TestException(Exception):
    pass


class Test_transaction(TestCase):

    def test_exception_rolls_back_changes(self):
        try:
            with db.transaction():
                f = m.get(F, TEST_UUID)
                m.delete(f)

                raise TestException()
        except TestException:
            f = m.get(F, TEST_UUID)
            self.assertEqual('?', f.future)
            return

        self.fail('expected TestException was not raised')

    def test_internal_exception_rolls_back_internal_only_changes(self):
        def failing_internal_transaction():
            b_id = None
            try:
                with db.transaction():
                    b_id = insert(B, b='new B')

                    raise TestException()
            except TestException:
                return b_id

            self.fail('expected TestException was not raised')

        with db.transaction():
            a_id = insert(A, a='new A')

            b_id = failing_internal_transaction()
            self.assertIsNotNone(b_id)

        a = m.get(A, a_id)
        self.assertEqual('new A', a.a)
        self.assertRaises(LookupError, m.get, B, b_id)

    def test_nested_w_outer_exception_rolls_back_all_changes(self):
        try:
            with db.transaction():
                a_id = insert(A, a='new A')

                with db.transaction():
                    b_id = insert(B, b='new B')

                raise TestException()
        except TestException:
            self.assertRaises(LookupError, m.get, A, a_id)
            self.assertRaises(LookupError, m.get, B, b_id)
            return

        self.fail('expected TestException was not raised')

db2 = Database()


@database(db2)
@table_name('aa')
@storable_pk_autoinc
class A2(A):
    pass


class Test_multiple_databases(unittest.TestCase):

    def setUp(self):
        db.connect(':memory:')
        db.connection.executescript(
            '''\
            create table aa(id integer primary key, a);
            insert into aa(id, a) values (0, 'A() in db at 0');
            ''')
        db2.connect(':memory:')
        db2.connection.executescript(
            '''\
            create table aa(id integer primary key, a);
            insert into aa(id, a) values (0, 'A2() in db2 at 0');
            ''')
        # db and db2 are two different in-memory databases
        # see https://www.sqlite.org/inmemorydb.html

    def test_meta(self):
        self.assertEqual(
            get_class_meta(A).table_name,
            get_class_meta(A2).table_name)

        self.assertNotEqual(
            get_class_meta(A).database,
            get_class_meta(A2).database)

    def test_get(self):
        a = m.get(A, 0)
        a2 = m.get(A2, 0)

        self.assertEqual('A() in db at 0', a.a)
        self.assertEqual('A2() in db2 at 0', a2.a)

    def test_delete(self):
        a2 = m.get(A2, 0)
        m.delete(a2)

        m.get(A, 0)
        self.assertRaises(LookupError, m.get, A2, 0)

    def test_update(self):
        a2 = m.get(A2, 0)
        a2.a = 'updated'
        m.save(a2)

        a = m.get(A, 0)
        a2 = m.get(A2, 0)

        self.assertEqual('A() in db at 0', a.a)
        self.assertEqual('updated', a2.a)

    def test_create(self):
        a2 = A2()
        a2.a = 'created'
        m.create(a2)

        self.assertEqual('created', m.get(A2, a2.id).a)
        self.assertRaises(LookupError, m.get, A, a2.id)


class Test_table_exists(TestCase):

    def test_existing_table(self):
        self.assertTrue(m.table_exists(A))
        self.assertTrue(m.table_exists(B))
        self.assertTrue(m.table_exists(AB))
        self.assertTrue(m.table_exists(F))

    def test_nonexisting_table(self):
        db.connection.executescript(
            '''\
            drop table aa;
            drop table bs;
            drop table x;
            drop table fs;
            ''')
        self.assertFalse(m.table_exists(A))
        self.assertFalse(m.table_exists(B))
        self.assertFalse(m.table_exists(AB))
        self.assertFalse(m.table_exists(F))


class Test_create_table(unittest.TestCase):

    def test_usable_table_is_created(self):
        db.connect(':memory:')
        self.assertFalse(m.table_exists(A))

        m.create_table(A)

        self.assertTrue(m.table_exists(A))
        insert(A, a='new value')

    def test_custom_database(self):
        db2.connect(':memory:')
        self.assertFalse(m.table_exists(A2))

        m.create_table(A2)

        self.assertTrue(m.table_exists(A2))
        insert(A2, a='new value')

    def test_table_constraint(self):
        @m.sql_constraint('''CHECK (a IN ('A', 'ABC'))''')
        @m.sql_constraint('''CHECK (length(a) < 2)''')
        @m.table_name('a_s')
        @storable_pk_autoinc
        class A(object):
            a = Field()

        db.connect(':memory:')
        m.create_table(A)

        insert(A, a='A')
        self.assertRaises(m.IntegrityError, insert, A, a='B')
        self.assertRaises(m.IntegrityError, insert, A, a='ABC')
        insert(A, a='A')


class PlainA(object):
    def __init__(self):
        super(PlainA, self).__init__()
        self.a_init = True


@storable_pk_autoinc
class SA(PlainA):
    def __init__(self):
        super(SA, self).__init__()
        self.sa_init = True


class Test_super_in_subclass(TestCase):

    def test_all_inits_are_called(self):
        sa = SA()
        self.assertTrue(sa.sa_init)
        self.assertTrue(sa.a_init)


if __name__ == '__main__':
    unittest.main()
