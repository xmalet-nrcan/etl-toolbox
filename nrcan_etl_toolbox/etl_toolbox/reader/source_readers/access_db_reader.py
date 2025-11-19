import pathlib
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, inspect, URL

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class MicrosoftAccessDatabaseReader(BaseDataReader):
    def __init__(self, file_path: str | pathlib.Path, db_user: str = None, db_password: str = None, **kwargs: Any):
        connection_string = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={file_path};ExtendedAnsiSQL=1;"
        if db_user is not None:
            connection_string += f"UID={db_user};"
        if db_password is not None:
            connection_string += f"PWD={db_password};"
        url = URL.create(
            "access+pyodbc",
            query={"odbc_connect": connection_string},
            username=db_user,
            password=db_password,
        )
        self._engine = create_engine(url)
        self._inspector = inspect(self._engine)
        self._table_names = self._inspector.get_table_names()
        self._table_name: str = None
        super().__init__(input_source=self._engine)

    @property
    def get_list_of_tables(self):
        return self._table_names

    def __del__(self):
        self._inspector.clear_cache()
        self._engine.dispose()

    def read_table(
            self,
            table_name: str = None,
            where_query: str = None,
            limit: int = None,
            set_internal_dataframe: bool = False,
            cols_to_lowercase=True,
            **kwargs,
    ) -> pd.DataFrame:

        if table_name not in self.get_list_of_tables:
            raise ValueError(f"Table {table_name} not found in Access Database file.")

        self._table_name = table_name
        df = self._read_data_from_query_parameters(table_name=table_name, where_query=where_query, limit=limit, **kwargs)

        if cols_to_lowercase:
            df = self._to_lowercase_columns(df)

        if set_internal_dataframe:
            self._dataframe = df
            return self._dataframe
        else:
            return df

    def _read_data_from_query_parameters(self, table_name: str | None,
                                         where_query: str | None,
                                         limit: int | None,
                                            **kwargs
                                         ) -> pd.DataFrame:
        query = f'SELECT * FROM "{table_name}"'

        if where_query is not None:
            query += f" WHERE {where_query}"
        if limit is not None:
            query += f" LIMIT {limit}"
        return pd.read_sql(query, self._engine, **kwargs)

    @property
    def columns(self) -> list:
        """Returns a list of column names for the current table or
        the column names for all tables in the."""
        match self.dataframe:
            case pd.DataFrame():
                return list(self._dataframe.columns)
            case dict():
                return [{i: list(self._dataframe[i].columns)} for i in self._dataframe]
            case _:
                return [{i: list(self.read_table(i).columns)} for i in self.get_list_of_tables]

    def read_all_database(self, cols_to_lowercase=True, set_internal_dataframe=False) -> dict[str, pd.DataFrame]:
        """Reads all tables from the Access database and returns them as a dictionary of DataFrames."""
        all_tables_data = {}
        for table_name in self.get_list_of_tables:
            all_tables_data[table_name] = self.read_table(table_name=table_name, cols_to_lowercase=cols_to_lowercase)
        if set_internal_dataframe:
            self._dataframe = all_tables_data
            return self._dataframe
        return all_tables_data
