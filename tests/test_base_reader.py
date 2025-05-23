import pandas as pd
import pytest

from nrcan_etl_toolbox.etl_toolbox.reader.base_reader import BaseDataReader


def test_cannot_instantiate_base_reader():
    with pytest.raises(AttributeError):
        BaseDataReader(input_source="test").columns  # noqa: B018


def test_base_reader_default_input_source():
    reader = DummyDataReader(input_source=None)
    assert reader._input_source is None


def test_base_reader_input_source_validation():
    input_source = "test_source"
    reader = DummyDataReader(input_source=input_source)
    assert reader._input_source == input_source


class DummyDataReader(BaseDataReader):
    def __init__(self, input_source, data=None):
        super().__init__(input_source)
        self._input_data = data

    def _read_data(self, **kwargs):
        if self._input_data is None:
            raise ValueError("No data provided")
        self._dataframe = pd.DataFrame(self._input_data)


def test_base_datareader_initialization():
    reader = DummyDataReader(input_source="dummy_source")
    assert reader._input_source == "dummy_source"
    assert reader._dataframe is None


def test_base_datareader_read_data():
    data = {"column1": [1, 2, 3], "column2": [4, 5, 6]}
    reader = DummyDataReader(input_source="dummy_source", data=data)
    df = reader.dataframe
    assert isinstance(df, pd.DataFrame)
    assert df.equals(pd.DataFrame(data))


def test_base_datareader_columns_property():
    data = {"column1": [1, 2, 3], "column2": [4, 5, 6]}
    reader = DummyDataReader(input_source="dummy_source", data=data)
    columns = reader.columns
    assert isinstance(columns, list)
    assert columns == ["column1", "column2"]


def test_base_datareader_raises_error_when_no_data():
    reader = DummyDataReader(input_source="dummy_source")
    with pytest.raises(ValueError, match="No data provided"):
        _ = reader.dataframe
