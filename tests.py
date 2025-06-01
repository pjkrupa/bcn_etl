import pytest
from sql_queries import create_table

def test_create_table():
    table_name = 'bcn_names'
    columns = {'name': 'VARCHAR', 'sex': 'INT', 'value': 'REAL', 'frequency': 'INT', 'year': 'INT'}
    assert create_table(table_name, columns) == "CREATE TABLE bcn_names (name VARCHAR, sex INT, value REAL, frequency INT, year INT);"



#sql = create_table('bcn_names', columns)