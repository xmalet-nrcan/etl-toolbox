import pandas as pd
import pytest
from unittest.mock import patch, MagicMock
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker
from testcontainers.postgres import PostgresContainer

from nrcan_etl_toolbox.etl_toolbox.reader.reader_factory import ReaderFactory
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers import PostGisTableDataReader

postgres = PostgresContainer('postgis/postgis:17-master')
@pytest.fixture(scope="module", autouse=True)
def setup(request):
    postgres.start()

    def remove_container():
        postgres.stop()

    request.addfinalizer(remove_container)

@pytest.mark.parametrize(
    "input_source,extension,expected_class",
    [
        ("file.xlsx", ".xlsx", "ExcelReader"),
        ("file.xls", ".xls", "ExcelReader"),
        ("file.gpkg", ".gpkg", "GeoPackageDataReader"),
        ("file.csv", ".csv", "CSVReader"),
        ("file.json", ".json", "JSONReader"),
        ("file.shp", ".shp", "ShapefileReader"),
    ]
)
def test_create_reader_file_types(input_source, extension, expected_class):
    with patch("nrcan_etl_toolbox.etl_toolbox.reader.reader_factory.ExcelReader") as ExcelReader, \
         patch("nrcan_etl_toolbox.etl_toolbox.reader.reader_factory.GeoPackageDataReader") as GeoPackageDataReader, \
         patch("nrcan_etl_toolbox.etl_toolbox.reader.reader_factory.CSVReader") as CSVReader, \
         patch("nrcan_etl_toolbox.etl_toolbox.reader.reader_factory.JSONReader") as JSONReader, \
         patch("nrcan_etl_toolbox.etl_toolbox.reader.reader_factory.ShapefileReader") as ShapefileReader:

        # Map expected class name to the correct mock
        class_map = {
            "ExcelReader": ExcelReader,
            "GeoPackageDataReader": GeoPackageDataReader,
            "CSVReader": CSVReader,
            "JSONReader": JSONReader,
            "ShapefileReader": ShapefileReader,
        }
        mock_reader = MagicMock()
        class_map[expected_class].return_value = mock_reader

        factory = ReaderFactory(input_source)
        assert factory.reader is mock_reader
        class_map[expected_class].assert_called_once_with(input_source)

def test_create_reader_postgis_engine():
    with patch("nrcan_etl_toolbox.etl_toolbox.reader.reader_factory.PostGisTableDataReader") as PostGisTableDataReader:
        engine = create_engine(postgres.get_connection_url(), echo=False, future=True)
        mock_reader = MagicMock()
        PostGisTableDataReader.return_value = mock_reader

        factory = ReaderFactory(engine, schema="myschema", table_name="mytable")
        assert factory.reader is mock_reader
        PostGisTableDataReader.assert_called_once_with(engine, schema="myschema", table_name="mytable")

def test_create_reader_postgis_session():
    session = Session(bind=create_engine(postgres.get_connection_url(), echo=False, future=True))

    factory = ReaderFactory(session, schema="myschema", table_name="mytable")
    assert isinstance(factory.reader, PostGisTableDataReader)
    # PostGisTableDataReader.assert_called_once_with(session, schema="myschema", table_name="mytable")

def test_create_reader_postgis_table_name():
    """
    Test the creation of a PostGisTableDataReader with a connection , table_name and schema.
    """
    # Simule un moteur SQLAlchemy pour le test
    engine = create_engine(postgres.get_connection_url())
    with engine.connect() as conn:


        # Création de la factory avec le nom de la table
        factory = ReaderFactory(conn,  table_name="ma_table_postgis", schema="public")

        # Vérifie que le lecteur créé est bien un PostGisTableDataReader
        assert isinstance(factory.reader, PostGisTableDataReader)
        assert factory.reader.table_name == "ma_table_postgis"
        assert factory.reader.schema == "public"


def test_create_reader_unsupported_extension():
    with pytest.raises(ValueError, match="Type de fichier non pris en charge"):
        ReaderFactory("file.unsupported")

def test_dataframe_and_columns_properties():
    # Simule un reader avec les propriétés nécessaires
    mock_reader = MagicMock()
    df_for_test = pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    mock_reader.dataframe = df_for_test.copy(deep=True)
    mock_reader.columns = ["col1", "col2"]

    with patch("nrcan_etl_toolbox.etl_toolbox.reader.reader_factory.ReaderFactory._create_reader", return_value=mock_reader):
        factory = ReaderFactory("file.csv")
        assert factory.data.equals(df_for_test)
        assert factory.dataframe().equals(df_for_test)
        assert factory.columns == ["col1", "col2"]


