import pandas as pd
import pytest
import pathlib
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.geopackage_reader import GeoPackageDataReader

data_folder = pathlib.Path(__file__).parent / "data"

@pytest.fixture
def input_test_data() -> pathlib.Path:
    return data_folder / "test_data.gpkg"

@pytest.fixture
def empty_geopackage() -> pathlib.Path:
    return data_folder / "empty_geopackage.gpkg"

@pytest.fixture
def invalid_geopackage_source() -> pathlib.Path:
    invalid_path = data_folder / "invalid_geopackage.gpkg"
    if invalid_path.exists():
        invalid_path.unlink()  # Ensure the file does not exist
    return invalid_path

def test_geopackage_reader_initialization(input_test_data):
    reader = GeoPackageDataReader(input_source=input_test_data)
    assert reader._input_source == input_test_data
    assert reader._dataframe is None
    assert len(reader.layers) == 2 # the test data has 3 layers but only 2 are feature layers
    assert reader.layers[0] != reader.layers[1]
    assert 'bdgeo_camion' in reader.layers
    assert 'random_points' in reader.layers

def test_geopackage_reader_switch_layers(input_test_data):
    reader = GeoPackageDataReader(input_source=input_test_data)
    layers = reader.layers
    for layer in layers:
        df = reader.read_layer(layer)
        assert isinstance(df, pd.DataFrame)
        assert not df.empty
        assert reader._layer == layer

def test_geopackage_reader_empty_geopackage(empty_geopackage):
    reader = GeoPackageDataReader(input_source=empty_geopackage)
    assert reader.layers == []  # No layers should be found in an empty GeoPackage
    with pytest.raises(ValueError):
        reader.read_layer("non_existent_layer")

def test_geopackage_reader_invalid_layer(input_test_data):
    reader = GeoPackageDataReader(input_source=input_test_data)
    with pytest.raises(ValueError, match="Layer 'non_existent_layer' not found in the GeoPackage. Available layers:"):
        reader.read_layer("non_existent_layer")


def test_geopackage_reader_no_layer_provided(input_test_data):
    reader = GeoPackageDataReader(input_source=input_test_data)
    with pytest.raises(ValueError, match="Layer name must be provided to read data from a GeoPackage."):
        reader.read_layer(None)



def test_geopackage_reader_non_feature_layer(invalid_geopackage_source):
    with pytest.raises(AssertionError):
        reader = GeoPackageDataReader(input_source=invalid_geopackage_source)

