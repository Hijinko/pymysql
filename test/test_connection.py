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
        self._values = [('Jon', 'Doe'), ('Jane', 'Doe')]
        self._keys = [('id INT NOT NULL PRIMARY KEY AUTO_INCREMENT'),
                      ('fname VARCHAR(20) NOT NULL'),
                      ('lname VARCHAR(20) NOT NULL')]
        self._table = 'test_table'
        self._database = 'Test'
        self._conn = Connection(password_required=True)
        self._conn.create_database(self._database)
        self._conn.create_table(self._table, self._keys)
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
        '''
        @brief: test the creation of a table
        '''
        self._conn.create_table(self._table, self._keys)
        query = f"DESCRIBE {self._table}"
        descriptions = self._conn.custom_query(query)
        self.assertTrue(_key_in_return(descriptions, 'id'))
        self.assertTrue(_key_in_return(descriptions, 'fname'))
        self.assertTrue(_key_in_return(descriptions, 'lname'))
        self._conn.close()

    def test_insert(self):
        '''
        @brief test the ability to insert data into the table
        '''
        self._conn.insert(self._values)
        names = self._conn.select_all()
        self.assertTrue(_key_in_return(names, self._values[0][0]))
        self._conn.close()

    def test_select_all(self):
        '''
        @brief test the ability to select all the data from a table
        '''
        data = self._conn.select_all()
        self.assertTrue(_key_in_return(data, self._values[0][0]))
        self.assertTrue(_key_in_return(data, self._values[0][1]))
        self.assertTrue(_key_in_return(data, self._values[1][0]))
        self.assertTrue(_key_in_return(data, self._values[1][1]))
        self._conn.close()
         
    def test_clear_table(self):
        '''
        @brief test the ability to clear a table of all its contents 
        '''
        self._conn.clear_table()
        data = self._conn.select_all()
        self.assertFalse(_key_in_return(data, self._values[0][0]))

    def test_delete_table(self):
        '''
        @brief test the ability to delete a table
        '''
        self._conn.delete_table()
        query = 'SHOW TABLES'
        tables = self._conn.custom_query(query)
        self.assertFalse(_key_in_return(tables, self._database))


if __name__ == '__main__':
    unittest.main()
