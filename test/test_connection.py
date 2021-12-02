import unittest
from pymysql import Connection

class TestConnection(unittest.TestCase):
    def setUp(self):
        '''
        @brief: create the connection class for each tests
        '''
        self._conn = Connection(password_required=True)

    def test_connection_init(self):
        '''
        @brief test the initialization of the connection class
        '''
        self.assertTrue(isinstance(self._conn, Connection))

    def test_create_database(self):
        '''
        @brief: test the createion of a database
        '''
        self._conn.create_database('test')
        self._conn.close()

    def test_create_table(self):
        keys = [('id INT NOT NULL PRIMARY KEY AUTO_INCREMENT'),
                ('fname VARCHAR(20) NOT NULL'),
                ('lname VARCHAR(20) NOT NULL')]
        self._conn.create_table('test_table', keys)

    def test_insert(self):
        values = [('Jon', 'Doe'), ('Jane', 'Doe')]
        self._conn.insert(values)

    def test_select_all(self):
        self._conn.select_all()

    def test_clear_table(self):
        self._conn.clear_table()

    def test_delete_table(self):
        self._conn.delete_table()

if __name__ == '__main__':
    unittest.main()
