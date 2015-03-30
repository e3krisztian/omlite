import unittest

import omlite
from omlite import Model, Field


class A(Model):
    sqlite3_table_name = 'aa'
    a = Field()


class B(Model):
    b = Field()


class X(A, B):
    x = Field()


def given_a_database():
    omlite.connect(':memory:')
    omlite.connection.executescript(
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
