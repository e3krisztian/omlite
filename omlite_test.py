import unittest

import omlite as m
from omlite import Model, Field, UUIDModel


class A(Model):
    sqlite3_table_name = 'aa'
    a = Field()


class B(Model):
    b = Field()


class AB(A, B):
    sqlite3_table_name = 'x'
    x = Field()


TEST_UUID = '9764d716-2b5b-4a6f-9c75-621377a80028'


def given_a_database():
    m.connect(':memory:')
    m.connection.executescript(
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


class Test_Model_READ(TestCase):

    def test_get_sqlite3_table_name(self):
        self.assertEqual('model', Model.get_sqlite3_table_name())
        self.assertEqual('aa', A.get_sqlite3_table_name())
        self.assertEqual('b', B.get_sqlite3_table_name())

    def test_by_id(self):
        a = A.by_id(0)

        self.assertEqual(0, a.id)
        self.assertEqual('A() in db at 0', a.a)
        self.assertIsInstance(a, A)

        b = B.by_id(2)

        self.assertEqual(2, b.id)
        self.assertEqual('B() in db at 2', b.b)
        self.assertIsInstance(b, B)


class Test_Model_CREATE(TestCase):

    def test(self):
        a = A()
        a.a = 'A created in db'
        a.save()

        a_from_db = A.by_id(a.id)

        self.assertEqual('A created in db', a_from_db.a)


class Test_Model_UPDATE(TestCase):

    def test(self):
        a = A.by_id(0)
        a.a = 'overwritten field'
        a.save()

        a_from_db = A.by_id(0)
        self.assertEqual('overwritten field', a_from_db.a)
        self.assertNotEqual(id(a), id(a_from_db))


class Test_Model_DELETE(TestCase):

    def test_deleted(self):
        a = A.by_id(0)
        a.delete()

        self.assertIsNone(a.id)
        self.assertRaises(LookupError, A.by_id, 0)

    def test_deleted_can_be_resaved_with_new_id(self):
        a = A.by_id(0)
        a.delete()

        a.save()

        a_from_db = A.by_id(a.id)
        self.assertEqual(a.a, a_from_db.a)


class Test_Model_inheritance(TestCase):

    def test_by_id(self):
        ab = AB.by_id(2)

        self.assertEqual(ab.b, 'X() in db at 2')

    def test_update(self):
        ab = AB.by_id(2)
        ab.a = 'persisted attribute'
        ab.save()

        ab_from_db = AB.by_id(2)

        self.assertEqual(ab_from_db.b, 'X() in db at 2')
        self.assertEqual(ab.a, 'persisted attribute')


class F(UUIDModel):
    future = Field()


class Test_UUIDModel(TestCase):

    def test_create(self):
        f = F()
        f.future = 'newly saved'
        f.save()

        from_db = F.by_id(f.id)
        self.assertEqual('newly saved', from_db.future)

    def test_update(self):
        f = F.by_id(TEST_UUID)
        f.future = 'unknown'
        f.save()

        from_db = F.by_id(TEST_UUID)
        self.assertEqual('unknown', from_db.future)

    def test_delete(self):
        f = F.by_id(TEST_UUID)
        f.delete()

        self.assertIsNone(f.id)
        self.assertRaises(LookupError, F.by_id, TEST_UUID)

    def test_by_id(self):
        from_db = F.by_id(TEST_UUID)
        self.assertEqual('?', from_db.future)


class TestException(Exception):
    pass


class Test_transaction(TestCase):

    def test_exception_rolls_back_changes(self):
        try:
            with m.transaction():
                f = F.by_id(TEST_UUID)
                f.delete()

                raise TestException()
        except TestException:
            f = F.by_id(TEST_UUID)
            self.assertEqual('?', f.future)
            return

        self.fail('expected TestException was not raised')

    def test_internal_exception_rolls_back_internal_only_changes(self):
        def failing_internal_transaction():
            b_id = None
            try:
                with m.transaction():
                    b = B()
                    b.b = 'new B'
                    b.save()
                    b_id = b.id

                    raise TestException()
            except TestException:
                return b_id

            self.fail('expected TestException was not raised')

        with m.transaction():
            a = A()
            a.a = 'new A'
            a.save()
            a_id = a.id

            b_id = failing_internal_transaction()
            self.assertIsNotNone(b_id)

        a = a.by_id(a_id)
        self.assertEqual('new A', a.a)
        self.assertRaises(LookupError, B.by_id, b_id)

    def test_nested_w_outer_exception_rolls_back_all_changes(self):
        try:
            with m.transaction():
                a = A()
                a.a = 'new A'
                a.save()
                a_id = a.id

                with m.transaction():
                    b = B()
                    b.b = 'new B'
                    b.save()
                    b_id = b.id

                raise TestException()
        except TestException:
            self.assertRaises(LookupError, A.by_id, a_id)
            self.assertRaises(LookupError, B.by_id, b_id)
            return

        self.fail('expected TestException was not raised')


if __name__ == '__main__':
    unittest.main()
