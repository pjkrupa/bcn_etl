import os
import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from dataclasses import dataclass
import psycopg2
from psycopg2 import sql

@dataclass
class Database:
    db_user: str
    db_host: str
    db_name: str

def get_file_names(path: str) -> list:
    return [entry.name for entry in os.scandir(path) if entry.is_file() and entry.name.endswith(".csv")]

def csv_to_df(path: str, file_name: str):
    my_csv = os.path.join(path, file_name)
    return pd.read_csv(my_csv)

def table_exists(db_config: Database, table_name: str) -> bool:
    with psycopg2.connect(f"dbname={db_config.db_name} user={db_config.db_user} host={db_config.db_host}") as conn:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass(%s);", (table_name,))
        return cur.fetchone()[0] is not None

def insert_df(
        df: DataFrame, 
        table_name: str,
        db_config: Database):

    engine = create_engine(f"postgresql+psycopg2://{db_config.db_user}@{db_config.db_host}/{db_config.db_name}")
    try:
        df.to_sql(table_name, engine, if_exists="append", index=False)
        return True
    except Exception as e:
        print(f"Was not able to create table with df insert: {e}")
        return False

def copy_package_to_db(path: str, package_name: str, db_config: Database):
    """
    Calling copy_package_to_db creates a PostgreSQL table with inferred schema and
    copies all the CSV files in a package to that table.

    ARGUMENTS:
        path: the root path defined by the user where downloaded CSVs are saved.
        package_name: name of the package, which is also a directory name and used to name the PostgreSQL table
        db_config: config for connecting to the database
    RETURNS:
        None
    """
    if table_exists(
        db_config=db_config,
        table_name=package_name
        ):
        print(f"Table {package_name} already exists. For now, to avoid duplication, we are only loading to fresh tables.")
        return
    
    with psycopg2.connect(f"dbname={db_config.db_name} user={db_config.db_user} host={db_config.db_host}") as conn:
        cur = conn.cursor()
        data_dir = os.path.join(path, package_name)
        file_names = get_file_names(data_dir)
        sample_file = file_names.pop(0)
        sample_df = csv_to_df(
            path=data_dir,
            file_name=sample_file
            )
        if insert_df(
            df=sample_df, 
            table_name=package_name,
            db_config=db_config
            ):

            for file in file_names:
                file_path = os.path.join(path, package_name, file)
                with open(file_path, "r") as f:
                    copy_sql = sql.SQL("COPY {} FROM STDIN WITH CSV HEADER").format(
                        sql.Identifier(package_name)
                        )
                    cur.copy_expert(copy_sql, f)
        conn.commit()