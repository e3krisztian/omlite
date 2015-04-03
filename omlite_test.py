import unittest

from omlite import db, Field
from omlite import storable_pk_autoinc, storable_pk_netaddrtime_uuid1
from omlite import get_class_meta, table_name


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
    db.save(obj)
    return obj.id


def given_a_database():
    db.connect(':memory:')
    db.connection.executescript(
        '''\
        create table aa(id integer primary key, a);
        insert into aa(id, a) values (0, 'A() in db at 0');
        insert into aa(id, a) values (1, 'A() in db at 1');
        create table b(id integer primary key, b);
        insert into b(id, b) values (0, 'B() in db at 0');
        insert into b(id, b) values (2, 'B() in db at 2');
        create table x(id integer primary key, a, b, x);
        insert into x(id, b) values (2, 'X() in db at 2');
        create table f(id varchar primary key, future);
        insert into f(id, future)
            values ('9764d716-2b5b-4a6f-9c75-621377a80028', '?');
        ''')


class TestCase(unittest.TestCase):

    def setUp(self):
        given_a_database()


class Test_meta(unittest.TestCase):

    def test_table_name(self):
        self.assertEqual('aa', get_class_meta(A).table_name)
        self.assertEqual('b', get_class_meta(B).table_name)


class Test_storable_READ(TestCase):

    def test_by_id(self):
        a = db.get(A, 0)

        self.assertEqual(0, a.id)
        self.assertEqual('A() in db at 0', a.a)
        self.assertIsInstance(a, A)

        b = db.get(B, 2)

        self.assertEqual(2, b.id)
        self.assertEqual('B() in db at 2', b.b)
        self.assertIsInstance(b, B)


class Test_storable_CREATE(TestCase):

    def test(self):
        a = A()
        a.a = 'A created in db'
        db.save(a)

        a_from_db = db.get(A, a.id)

        self.assertEqual('A created in db', a_from_db.a)


class Test_storable_UPDATE(TestCase):

    def test(self):
        a = db.get(A, 0)
        a.a = 'overwritten field'
        db.save(a)

        a_from_db = db.get(A, 0)
        self.assertEqual('overwritten field', a_from_db.a)
        self.assertNotEqual(id(a), id(a_from_db))


class Test_storable_DELETE(TestCase):

    def test_deleted(self):
        a = db.get(A, 0)
        db.delete(a)

        self.assertIsNone(a.id)
        self.assertRaises(LookupError, db.get, A, 0)

    def test_deleted_can_be_resaved_with_new_id(self):
        a = db.get(A, 0)
        db.delete(a)

        db.save(a)

        a_from_db = db.get(A, a.id)
        self.assertEqual(a.a, a_from_db.a)


class Test_storable_inheritance(TestCase):

    def test_by_id(self):
        ab = db.get(AB, 2)

        self.assertEqual(ab.b, 'X() in db at 2')

    def test_update(self):
        ab = db.get(AB, 2)
        ab.a = 'persisted attribute'
        db.save(ab)

        ab_from_db = db.get(AB, 2)

        self.assertEqual(ab_from_db.b, 'X() in db at 2')
        self.assertEqual(ab.a, 'persisted attribute')


@storable_pk_netaddrtime_uuid1
class F(object):
    future = Field()


class Test_Storable_with_UUID_primary_key(TestCase):

    def test_create(self):
        id = insert(F, future='newly saved')

        from_db = db.get(F, id)
        self.assertEqual('newly saved', from_db.future)

    def test_update(self):
        f = db.get(F, TEST_UUID)
        f.future = 'unknown'
        db.save(f)

        from_db = db.get(F, TEST_UUID)
        self.assertEqual('unknown', from_db.future)

    def test_delete(self):
        f = db.get(F, TEST_UUID)
        db.delete(f)

        self.assertIsNone(f.id)
        self.assertRaises(LookupError, db.get, F, TEST_UUID)

    def test_by_id(self):
        from_db = db.get(F, TEST_UUID)
        self.assertEqual('?', from_db.future)


class TestException(Exception):
    pass


class Test_transaction(TestCase):

    def test_exception_rolls_back_changes(self):
        try:
            with db.transaction():
                f = db.get(F, TEST_UUID)
                db.delete(f)

                raise TestException()
        except TestException:
            f = db.get(F, TEST_UUID)
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

        a = db.get(A, a_id)
        self.assertEqual('new A', a.a)
        self.assertRaises(LookupError, db.get, B, b_id)

    def test_nested_w_outer_exception_rolls_back_all_changes(self):
        try:
            with db.transaction():
                a_id = insert(A, a='new A')

                with db.transaction():
                    b_id = insert(B, b='new B')

                raise TestException()
        except TestException:
            self.assertRaises(LookupError, db.get, A, a_id)
            self.assertRaises(LookupError, db.get, B, b_id)
            return

        self.fail('expected TestException was not raised')

if __name__ == '__main__':
    unittest.main()
