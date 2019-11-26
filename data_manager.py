import os
import psycopg2
import psycopg2.extras


def establish_connection(
        connection_data='postgres://dbmasteruser:W?SbU5f6MOS9CG2z1C*C)hz?)>SF|2l,@ls-32cbb4d180aaad0b359695120eedc91e6fca9dfa.clcnyrcuavnn.eu-central-1.rds.amazonaws.com:5432/postgres'):
    conn = psycopg2.connect(connection_data)
    conn.autocommit = True
    return conn


def get_connection_data(db_name=None):
    """
    Give back a properly formatted dictionary based on the environment variables values which are started
    with :MY__PSQL_: prefix

    :db_name: optional parameter. By default it uses the environment variable value.
    """
    if db_name is None:
        db_name = os.environ.get('MY_PSQL_DBNAME')

    return {
        'dbname': db_name,
        'user': os.environ.get('MY_PSQL_USER'),
        'host': os.environ.get('MY_PSQL_HOST'),
        'password': os.environ.get('MY_PSQL_PASSWORD')
    }


def execute_script_file(file_path):
    """
    Execute script file based on the given file path.
    Print the result of the execution to console.

    Example:
    > execute_script_file('db_schema/01_create_schema.sql')

    :file_path: Relative path of the file to be executed.
    """
    package_directory = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(package_directory, file_path)
    with open(full_path) as script_file:
        with establish_connection() as conn, \
                conn.cursor() as cursor:
            try:
                sql_to_run = script_file.read()
                cursor.execute(sql_to_run)
                print("{} script executed successfully.".format(file_path))
            except Exception as ex:
                print("Execution of {} failed".format(file_path))
                print(ex.args)


def execute_select(statement, variables=None):
    """
    Execute SELECT statement optionally parameterized

    Example:
    > execute_select('SELECT %(title)s; FROM shows', variables={'title': 'Codecool'})

    :statement: SELECT statement

    :variables:  optional parameter dict"""
    result_set = []
    with establish_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(statement, variables)
            result_set = cursor.fetchall()
    return result_set


def execute_dml_statement(statement, variables=None):
    """
    Execute data manipulation query statement (optionally parameterized)

    :statment: SQL statement

    :variables:  optional parameter dict"""
    result = None
    with establish_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(statement, variables)
            try:
                result = cursor.fetchone()
            except psycopg2.ProgrammingError as pe:
                pass
    return result
