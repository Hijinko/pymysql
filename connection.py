import getpass
import mysql.connector


class Connection:
    '''
    @brief: class for connecting to an sql database
    @parm database string name of the database to connect to 
    '''
    def __init__(self, database):
        self._database = database
        self._connection = None;
        self._table = None;
        self._get_connection() # creates the connection to the database
        self._cursor = self._connection.cursor()

    def _get_connection(self):
        '''
        @brief: creates the connection to the sql database, must be run before
         any other functions can be run on the class
        '''
        config = {'user': getpass.getuser(), 
                  'database': self._database,
                  'password': getpass.getpass()}
        self._connection = mysql.connector.connect(**config)

    def create_table(self, table_name, keys):
        '''
        @brief creates a table in the class database as long as the table does 
         notalready exist
        @param table_name string name of the table to create
        @parm keys a collection of lists or tuples that has the keys for 
         the table
        '''
        self._table = table_name
        sql = (f"CREATE TABLE IF NOT EXISTS {self._table}"
              f"({','.join(keys)})")
        self._cursor.execute(sql)

    def insert(self, values, auto_key=True):
        '''
        @brief adds values to the class table
        @param values list or tuple that has the values to insert 
        @param auto_key if set to False then the format string will account for
         an auto incrementing key else only specific values will be counted
        '''
        # get the format string for the insert
        if auto_key:
            fmt = f"{'%s, ' * len(values[0])}".strip().strip(',')
        else:
            fmt = f"0, {'%s, ' * len(values[0])}".strip().strip(',')
        sql = f"INSERT INTO {self._table} VALUES ({fmt})" 
        self._cursor.executemany(sql, values)
        self._connection.commit()

    def select_all(self):
        '''
        @brief gets all the values from the table
        @return returns all the table rows in a 
        '''
        sql = f"SELECT * FROM {self._table}"
        self._cursor.execute(sql)
        data = (row for index, row in enumerate(self._cursor.fetchall()))
        return data

    def clear_table(self):
        '''
        @brief deletes all the records from the class table
        '''
        sql = f"DELETE FROM {self._table}"
        self._cursor.execute(sql)
        self._connection.commit()

    def delete_table(self):
        '''
        @brief drops the class table from the databse
        '''
        sql = f"DROP TABLE IF EXISTS {self._table}"
        self._cursor.execute(sql)
        self._connection.commit()

    def close(self):
        '''
        @brief closes the class connection
        '''
        self._cursor.close()
        self._connection.close()

if __name__ == '__main__':
    conn = Connection('kellis')
    conn.close()
