import geopandas as gpd
import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class PostGisTableDataReader(BaseDataReader):
    def __init__(self, input_source: Engine | Session, schema: str, table_name: str, geometry_column_name: str = None):
        self._table_name = table_name
        self._schema = schema
        self._geometry_column_name = geometry_column_name
        super().__init__(input_source=input_source)

    @property
    def table_name(self):
        return self._table_name

    @property
    def schema(self):
        return self._schema

    @property
    def formatted_table_name(self):
        return f"{self._schema}.{self._table_name}" if self._schema else self._table_name

    def _read_data(self):
        query = f"select * from {self._schema}.{self._table_name}"
        match self._input_source:
            case Session():
                with self._input_source.begin() as session:
                    self._read_database(query, session.connection())
            case Engine():
                self._read_database(query, self._input_source)

    def _read_database(self, query, con):
        if self._geometry_column_name is None:
            self._dataframe = pd.read_sql(query, con)
        else:
            self._dataframe = gpd.read_postgis(query, con=con, geom_col=self._geometry_column_name)
