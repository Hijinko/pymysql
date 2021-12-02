import unittest
from pymysql import Connection

def _key_in_return(data, key):
    for value in data:
        if key in value:
            return True
    return False


class TestConnection(unittest.TestCase):
    def setUp(self):
        '''
        @brief: create the connection class for each tests
        '''
        self._table = 'test_table'
        self._database = 'Test'
        self._conn = Connection(self._database, password_required=True)
        self._conn.table = self._table

    def test_connection_init(self):
        '''
        @brief test the initialization of the connection class
        '''
        self.assertTrue(isinstance(self._conn, Connection))
        self._conn.close()

    def test_create_database(self):
        '''
        @brief: test the createion of a database
        '''
        self._conn.create_database(self._database)
        query = "SHOW DATABASES"
        databases = self._conn.custom_query(query)
        self.assertTrue(_key_in_return(databases, self._database))
        self._conn.close()

    def test_create_table(self):
        keys = [('id INT NOT NULL PRIMARY KEY AUTO_INCREMENT'),
                ('fname VARCHAR(20) NOT NULL'),
                ('lname VARCHAR(20) NOT NULL')]
        self._conn.create_table(self._table, keys)
        query = f"DESCRIBE {self._table}"
        descriptions = self._conn.custom_query(query)
        self.assertTrue(_key_in_return(descriptions, 'id'))
        self.assertTrue(_key_in_return(descriptions, 'fname'))
        self.assertTrue(_key_in_return(descriptions, 'lname'))
        self._conn.close()

"""
    def test_insert(self):
        values = [('Jon', 'Doe'), ('Jane', 'Doe')]
        self._conn.insert(values)
        query = "SELECT fname from {self._table}"
        names = self._conn.custom_query(query)
        print(f"NAMES ARE {names}")
        self.assertTrue(_key_in_return(names, values[0][0]))
        self._conn.close()
        
    def test_select_all(self):
        self._conn.select_all()

    def test_clear_table(self):
        self._conn.clear_table()

    def test_delete_table(self):
        self._conn.delete_table()
"""

if __name__ == '__main__':
    unittest.main()
