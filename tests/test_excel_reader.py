# tests/test_excel_reader.py

import os
import tempfile

import pandas as pd
import pytest

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.excel_reader import ExcelReader

SHEET_1_NAME = "Sheet1"
SHEET_2_NAME = "Sheet2"

SHEET_1_COL_1_NAME = "Column1_sh1"
SHEET_1_COL_2_NAME = "Column2_sh1"
SHEET_2_COL_1_NAME = "Column1_sh2"
SHEET_2_COL_2_NAME = "Column2_sh2"

SHEET_1_COL_1_DATA = [1, 2, 3]
SHEET_1_COL_2_DATA = [4, 5, 6]
SHEET_2_COL_1_DATA = ["Text1", "Text2", "Text3"]
SHEET_2_COL_2_DATA = ["Value1", "Value2", "Value3"]

# Data for the first sheet
SHEET_DATA_1 = {
    SHEET_1_COL_1_NAME: SHEET_1_COL_1_DATA,
    SHEET_1_COL_2_NAME: SHEET_1_COL_2_DATA,
}
# Data for the second sheet
SHEET_DATA_2 = {
    SHEET_2_COL_1_NAME: SHEET_2_COL_1_DATA,
    SHEET_2_COL_2_NAME: SHEET_2_COL_2_DATA,
}


def _create_temp_excel_file(data, sheet_names):
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as temp_file:
        try:
            with pd.ExcelWriter(temp_file) as writer:
                for sheet_name, sheet_data in zip(sheet_names, data, strict=False):
                    pd.DataFrame(sheet_data).to_excel(writer, index=False, sheet_name=sheet_name)
        except Exception as e:
            raise e
    return temp_file.name


def _create_temp_excel_file_two_sheets_different_data():
    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_file:
        try:
            # Write data to the Excel file
            with pd.ExcelWriter(temp_file.name) as writer:
                pd.DataFrame(SHEET_DATA_1).to_excel(writer, sheet_name=SHEET_1_NAME, index=False)
                pd.DataFrame(SHEET_DATA_2).to_excel(writer, sheet_name=SHEET_2_NAME, index=False)
        except Exception as e:
            raise e
    return temp_file.name


def test_excel_reader_initialization():
    temp_file = _create_temp_excel_file([{"A": [1, 2], "B": [3, 4]}], [SHEET_1_NAME])
    try:
        reader = ExcelReader(input_source=temp_file)
        assert isinstance(reader, ExcelReader)
        assert reader.sheet_name is None
        assert reader.skipfooter == 0
        assert reader.skiprows == 0
        assert isinstance(reader.list_sheet_names, list)
        del reader

    finally:
        os.unlink(temp_file)


def test_excel_reader_get_sheet_names():
    temp_file = _create_temp_excel_file([{"A": [1]}, {"B": [2]}], [SHEET_1_NAME, SHEET_2_NAME])
    try:
        reader = ExcelReader(input_source=temp_file)
        sheet_names = reader.list_sheet_names
        assert SHEET_1_NAME in sheet_names
        assert SHEET_2_NAME in sheet_names
        del reader

    finally:
        os.unlink(temp_file)


def test_excel_reader_read_sheet():
    temp_file = _create_temp_excel_file([{"A": [1, 2], "B": [3, 4]}], [SHEET_1_NAME])
    try:
        reader = ExcelReader(input_source=temp_file)
        dataframe = reader.read_sheet(sheet_name=SHEET_1_NAME, cols_to_lowercase=False)
        assert isinstance(dataframe, pd.DataFrame)
        assert dataframe.shape == (2, 2)
        assert list(dataframe.columns) == ["A", "B"]
        del reader
    finally:
        os.unlink(temp_file)


def test_excel_reader_invalid_sheet_name():
    temp_file = _create_temp_excel_file([{"A": [1]}], [SHEET_1_NAME])
    try:
        reader = ExcelReader(input_source=temp_file)
        with pytest.raises(ValueError, match="Sheet NonExistentSheet not found in Excel file."):
            reader.read_sheet(sheet_name="NonExistentSheet")
        del reader

    finally:
        os.unlink(temp_file)


def test_excel_reader_set_internal_dataframe():
    temp_file = _create_temp_excel_file([{"A": [1, 2], "B": [3, 4]}], [SHEET_1_NAME])
    try:
        reader = ExcelReader(input_source=temp_file)
        dataframe = reader.read_sheet(sheet_name=SHEET_1_NAME, set_internal_dataframe=True, cols_to_lowercase=False)
        assert isinstance(dataframe, pd.DataFrame)
        assert reader.dataframe.equals(dataframe)
        del reader

    finally:
        os.unlink(temp_file)


def test_excel_reader_read_sheet_change_internal_true():
    """
    This test demonstrates reading two different sheets from an Excel file
    created by the _create_temp_excel_file_two_sheets_different_data function.
    """
    # Create a temporary Excel file with two sheets containing different data
    temp_file = _create_temp_excel_file_two_sheets_different_data()

    try:
        reader = ExcelReader(input_source=temp_file)
        # 1) Test reading Sheet1
        all_sheets_df = reader.dataframe
        assert isinstance(all_sheets_df, dict), "Sheet1 data should be returned as a DataFrame."
        # assert df_sheet1.shape == (3, 2), "Sheet1 DataFrame should have 3 rows and 2 columns."
        # assert list(df_sheet1.columns) == list(SHEET_DATA_1.keys()), "Sheet1 column names should match."

        df_sheet1 = reader.read_sheet(sheet_name=SHEET_1_NAME, cols_to_lowercase=False)
        assert isinstance(df_sheet1, pd.DataFrame), "Sheet1 data should be returned as a DataFrame."
        assert df_sheet1.shape == (3, 2), "Sheet1 DataFrame should have 3 rows and 2 columns."
        assert list(df_sheet1.columns) == list(SHEET_DATA_1.keys()), "Sheet1 column names should match."
        assert all_sheets_df[SHEET_1_NAME].equals(df_sheet1), (
            "Sheet1 DataFrame should be the same as the one returned by read_sheet."
        )
        assert type(all_sheets_df) is not type(df_sheet1), (
            "Internal dataframe should be different from the one returned by read_sheet."
        )
        # Verify the actual content of Sheet1
        assert df_sheet1[SHEET_1_COL_1_NAME].tolist() == SHEET_1_COL_1_DATA, (
            "Sheet1 'Column1' values are not as expected."
        )
        assert df_sheet1[SHEET_1_COL_2_NAME].tolist() == SHEET_1_COL_2_DATA, (
            "Sheet1 'Column2' values are not as expected."
        )

        # Test set_internal_dataframe=True for SHEET_1
        reader.read_sheet(sheet_name=SHEET_1_NAME, set_internal_dataframe=True, cols_to_lowercase=False)
        assert reader.dataframe.equals(pd.DataFrame(SHEET_DATA_1)), (
            "Internal dataframe should be updated when read_sheet is called with set_internal_dataframe=True."
        )
        assert reader.list_sheet_names == [SHEET_1_NAME, SHEET_2_NAME], (
            "Sheet names should not be updated when read_sheet is called with set_internal_dataframe=True."
        )

        # 2) Test reading Sheet2
        df_sheet2 = reader.read_sheet(sheet_name=SHEET_2_NAME, cols_to_lowercase=False)
        assert isinstance(df_sheet2, pd.DataFrame), "Sheet2 data should be returned as a DataFrame."
        assert df_sheet2.shape == (3, 2), "Sheet2 DataFrame should have 3 rows and 2 columns."
        assert list(df_sheet2.columns) == list(SHEET_DATA_2.keys()), "Sheet2 column names should match."

        # Verify the actual content of Sheet2
        assert df_sheet2[SHEET_2_COL_1_NAME].tolist() == SHEET_2_COL_1_DATA, "Sheet2 'A' values are not as expected."
        assert df_sheet2[SHEET_2_COL_2_NAME].tolist() == SHEET_2_COL_2_DATA, "Sheet2 'B' values are not as expected."

        # Test set_internal_dataframe=True for SHEET_2
        reader.read_sheet(sheet_name=SHEET_2_NAME, set_internal_dataframe=True, cols_to_lowercase=False)
        assert reader.dataframe.equals(pd.DataFrame(SHEET_DATA_2)), (
            "Internal dataframe should be updated when read_sheet is called with set_internal_dataframe=True."
        )
        assert reader.list_sheet_names == [SHEET_1_NAME, SHEET_2_NAME], (
            "Sheet names should not be updated when read_sheet is called with set_internal_dataframe=True."
        )

        # Test set and reset internal dataframe
        reader.read_sheet(sheet_name=SHEET_2_NAME, set_internal_dataframe=True, cols_to_lowercase=False)
        reader.read_sheet(sheet_name=SHEET_1_NAME, set_internal_dataframe=True, cols_to_lowercase=False)
        reader.read_sheet(sheet_name=SHEET_2_NAME, set_internal_dataframe=False, cols_to_lowercase=False)
        assert reader.dataframe.equals(pd.DataFrame(SHEET_DATA_1)), (
            f"Internal dataframe should the same as {SHEET_1_NAME}"
        )
        assert reader.list_sheet_names == [SHEET_1_NAME, SHEET_2_NAME], (
            "Sheet names should not be updated when read_sheet is called with set_internal_dataframe=True."
        )

    finally:
        del reader
        # Remove the temporary file after the test
        os.unlink(temp_file)


def test_excel_reader_reset_internal_dataframe():
    temp_file = _create_temp_excel_file_two_sheets_different_data()

    try:
        reader = ExcelReader(input_source=temp_file)
        # 1) Test reading Sheet1
        all_sheets_df = reader.dataframe
        assert isinstance(all_sheets_df, dict), "Sheet1 data should be returned as a DataFrame."
        # assert df_sheet1.shape == (3, 2), "Sheet1 DataFrame should have 3 rows and 2 columns."
        # assert list(df_sheet1.columns) == list(SHEET_DATA_1.keys()), "Sheet1 column names should match."

        df_sheet1 = reader.read_sheet(sheet_name=SHEET_1_NAME, set_internal_dataframe=True)
        assert isinstance(df_sheet1, pd.DataFrame), "Sheet1 data should be returned as a DataFrame."

        reader.reset_internal_dataframe(with_sheet_name=False)
        assert isinstance(reader.dataframe, dict), (
            "Internal dataframe should be a dictionary when resetting with parameter with_sheet_name=False."
        )

        reader.sheet_name = SHEET_2_NAME
        reader.reset_internal_dataframe(with_sheet_name=True)
        assert isinstance(reader.dataframe, pd.DataFrame), (
            "Internal dataframe should be a DataFrame when resetting with parameter with_sheet_name=True."
        )
        assert reader.dataframe.equals(pd.DataFrame(SHEET_DATA_2)), (
            "Internal dataframe should be updated when reset_internal_dataframe is called with with_sheet_name=True."
        )
        assert reader.list_sheet_names == [SHEET_1_NAME, SHEET_2_NAME], (
            "Sheet names should not be updated when reset_internal_dataframe is called with with_sheet_name=True."
        )

    finally:
        del reader
        # Remove the temporary file after the test
        os.unlink(temp_file)
