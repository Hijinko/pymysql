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
        if database is not None:
            # creates the connection to the database
            self.get_connection(database)

    @property
    def table(self):
        '''@brief gets the current table of the connection'''
        return self._table

    @table.setter
    def table(self, table_name):
        '''@brief sets the current table of the connection'''
        sql = "SHOW TABLES"
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
            tables = [row for index, row in enumerate(cursor.fetchall())]
        for table in tables:
            if table_name in table:
                self._table = table_name
                break

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

    def get_connection(self, database=None):
        '''
        @brief: creates the connection to the sql database, must be run before
         any other functions can be run on the class
        @param database string that is the name of the database to connect
         to
        '''
        config = {'user': getpass.getuser()}
        if database is not None:
            config['database'] = database
            self._database = database

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
        with self._connection.cursor() as cursor:
            cursor = self._connection.cursor()
            cursor.execute(sql)
        self._connection.commit()
        self._connection.close()
        # now that the database is createad connect to the database
        self.get_connection(database)

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
        with self._connection.cursor() as cursor:
            cursor.execute(sql)

    def insert(self, values, auto_key=True):
        '''
        @brief adds values to the class table
        @param values list or tuple that has the values to insert
        @param auto_key if set to False then the format string will account for
         an auto incrementing key else only specific values will be counted
        '''
        # get the format string for the insert
        if auto_key:
            fmt = f"0, {'%s, ' * len(values[0])}".strip().strip(',')
        else:
            fmt = f"{'%s, ' * len(values[0])}".strip().strip(',')
        sql = f"INSERT INTO {self._table} VALUES ({fmt})"
        with self._connection.cursor() as cursor:
            cursor.executemany(sql, values)
        self._connection.commit()

    def select_all(self):
        '''
        @brief gets all the values from the table
        @return returns all the table rows in a
        '''
        sql = f"SELECT * FROM {self._table}"
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
            data = [row for index, row in enumerate(cursor.fetchall())]
        return data

    def clear_table(self):
        '''
        @brief deletes all the records from the class table
        '''
        sql = f"DELETE FROM {self._table}"
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
        self._connection.commit()

    def delete_table(self):
        '''
        @brief drops the class table from the databse
        '''
        sql = f"DROP TABLE IF EXISTS {self._table}"
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
        self._connection.commit()

    def custom_query(self, sql):
        '''
        @brief runs a custom_query on the connection
        @param sql string query to run on the connection
         the sql must be a query that retuns data such as SHOW, SELECT
        @return a list that has the query data
        '''
        with self._connection.cursor() as cursor:
            cursor.execute(sql)
            data = [row for index, row in enumerate(cursor.fetchall())]
        return data

    def close(self):
        '''
        @brief closes the class connection
        '''
        self._connection.close()


if __name__ == '__main__':
    conn = Connection('kellis')
    conn.close()
