
def create_table(table_name:str, fields:dict) -> str:
    table_name = table_name.replace(' ', '_')
    sql_statement = f'CREATE TABLE {table_name} ('
    for column, datatype in fields.items():
        sql_statement += f'{column} {datatype}, '
    sql_statement = sql_statement.rstrip(', ') + ');'
    return sql_statement


