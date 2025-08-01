import os
import pathlib

import pandas as pd
from sqlalchemy import Connection, Engine
from sqlalchemy.orm import Session

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.csv_reader import CSVReader
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.excel_reader import ExcelReader
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.geopackage_reader import GeoPackageDataReader
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.json_reader import JSONReader
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.postgis_reader import PostGisTableDataReader
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.shapefile_reader import ShapefileReader


class ReaderFactory:
    """
    This class acts as an Object Factory to instantiate various subclasses
    of BaseDataReader, depending on the data source type.
    """

    def __init__(
        self,
        input_source: str | Engine | Session | Connection | pathlib.Path = None,
        schema=None,
        table_name=None,
        **kwargs: dict[str, str] | None,
    ):
        self._input_source = input_source

        self._reader = self._create_reader(input_source, schema=schema, table_name=table_name, **kwargs)

    def dataframe(self) -> pd.DataFrame:
        return self.data

    @property
    def data(self) -> pd.DataFrame:
        return self._reader.dataframe

    @property
    def reader(self):
        return self._reader

    @property
    def columns(self):
        return self._reader.columns

    @staticmethod
    def _create_reader(input_source, schema=None, table_name=None, **kwargs) -> BaseDataReader:
        """
        Creates and returns an instance of a data reader.

        - If input_source is an Engine or Session (from SQLAlchemy),
          returns a PostGisTableDataReader.
        - Otherwise, it determines the reader type to use based on the file extension.

        Parameters
        ----------
        input_source :
            A file path or an object representing a data source.
        kwargs :
            Additional parameters passed to the reader’s constructor.

        Returns
        -------
        BaseDataReader :
            A reader object corresponding to the detected data source type.
        """
        # Check for SQLAlchemy-based sources
        if isinstance(input_source, (Engine, Session, Connection)):
            # Parameters (e.g., schema, table_name, etc.) can be passed via **kwargs
            return PostGisTableDataReader(input_source, schema=schema, table_name=table_name, **kwargs)

        # Handle file-based sources by extension
        _, extension = os.path.splitext(str(input_source).lower())
        match extension:
            case ".xlsx" | ".xls":
                return ExcelReader(input_source, **kwargs)
            case ".gpkg":
                return GeoPackageDataReader(input_source, **kwargs)
            case ".csv":
                return CSVReader(input_source, **kwargs)
            case ".json":
                return JSONReader(input_source, **kwargs)
            case ".shp":
                return ShapefileReader(input_source, **kwargs)
            case _:
                raise ValueError(f"Type de fichier non pris en charge ou source invalide : {extension}")
