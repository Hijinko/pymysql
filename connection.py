from pathlib import Path
import getpass
import mysql.connector


class Connection:
    '''
    @brief: class for connecting to an sql database
    @parm database string name of the database to connect to 
    @param password_required boolean value to specifify if the mysql connection
     requries a password
    @param password string that is the password for the database
    '''
    def __init__(self, database=None, password_required=False, password=None):
        self._database = database
        self._connection = None
        self._table = None
        self._password = password
        self._get_password()
        # if a database name is passed then create a new connection
        if database:
            self._get_connection() # creates the connection to the database
            self._cursor = self._connection.cursor()
    
    def _get_password(self):
        '''
        @brief: sets the password of the database user if it is required
        '''
        conf_file = Path.joinpath(Path.home(), '.my.cnf')
        # check if the conf file exists
        if conf_file.is_file():
            with open(conf_file, 'r') as cnf:
                data = cnf.readlines()[1].split('=')
            # the password in the file initially has additional quotes
            self._password = data[1].strip().strip('"')
        elif self._password is None and password_required:
            # the conf file is not present and a password is required
            elf._password = getpass.getpass() 
        else:
            # the conf file is not present and a password is not required
            # or was given
            self._password = password

    def _get_connection(self):
        '''
        @brief: creates the connection to the sql database, must be run before
         any other functions can be run on the class
        '''
        config = {'user': getpass.getuser(), 
                  'database': self._database}
        if self._password is not None:
            config['password'] = self._password
        self._connection = mysql.connector.connect(**config)

    def create_database(self, database):
        '''
        @brief: creates mysql database
        @param: database string name of the database to create
        '''
        # create the database only if it does not exist
        config = {'user': getpass.getuser()}
        if self._password is not None:
            config['password'] = self._password
        sql = f"CREATE DATABASE IF NOT EXISTS {database}"
        self._connection = mysql.connector.connect(**config)
        self._cursor = self._connection.cursor()
        self._cursor.execute(sql)
        self._connection.commit()
        self._cursor.close()
        self._connection.close()
        # now that the database is createad connect to the database
        self._get_connection()

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
