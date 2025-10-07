import pytest
from db_load import insert_df
from unittest.mock import MagicMock, patch

def test_insert_df_database_connection_string():
    db_config = MagicMock(db_user="user", db_host="host", db_name="db")
    

