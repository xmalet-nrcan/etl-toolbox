import pandas as pd

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class ExcelReader(BaseDataReader):
    """
    Class to read data from Excel files. Input is an Excel file path or an Excel file object.

    Other parameters are passed to pandas.read_excel() function.
    See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html for more information
    """

    def __init__(self, input_source, sheet_name=None, skiprows=0, skipfooter=0, **kwargs):
        super().__init__(input_source)

        self.skipfooter = skipfooter
        self.skiprows = skiprows
        self.sheet_name = sheet_name
        self._kwargs = kwargs
        self._original_file = pd.ExcelFile(
            self._input_source
        )  # 20250806 - Removed the `engine` parameter to allow pandas to choose the best engine automatically.
        self.sheet_name = sheet_name

    def __del__(self):
        if self._original_file is not None:
            self._original_file.close()
        del self._dataframe

    @property
    def list_sheet_names(self):
        return list(self._original_file.sheet_names)

    def read_sheet(
        self,
        sheet_name,
        set_internal_dataframe: bool = False,
        skiprows=0,
        skipfooter=0,
        cols_to_lowercase=False,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Reads a specified sheet from an Excel file and returns its contents as a pandas DataFrame.
        This method allows the user to optionally set the internal dataframe and also supports
        skipping rows from the top or bottom of the sheet while reading. It either updates the
        internal dataframe or directly returns the sheet data based on the value of
        `set_internal_dataframe`.

        Other parameters are passed to pandas.read_excel() function.
        See https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html for more information

        :param sheet_name: The name of the sheet within the Excel file to read.
        :type sheet_name: str
        :param set_internal_dataframe: A flag that indicates whether to update the internal dataframe
            with the sheet data. Defaults to False.
        :type set_internal_dataframe: bool
        :param skiprows: The number of rows to skip at the beginning when reading the sheet.
            Defaults to 0.
        :type skiprows: int
        :param skipfooter: The number of rows to skip at the end when reading the sheet.
            Defaults to 0.
        :type skipfooter: int
        :param kwargs: Additional arguments to pass to `pandas.read_excel` for customization.
        :return: A pandas DataFrame containing the data from the specified sheet.
        :rtype: pandas.DataFrame
        :raises ValueError: If the specified sheet_name does not exist in the Excel file.
        """
        if sheet_name not in self.list_sheet_names:
            raise ValueError(f"Sheet {sheet_name} not found in Excel file.")
        self.sheet_name = sheet_name
        if set_internal_dataframe:
            self._read_data(
                sheet_name=sheet_name,
                skiprows=skiprows,
                skipfooter=skipfooter,
                cols_to_lowercase=cols_to_lowercase,
                **kwargs,
            )
            return self._dataframe
        else:
            return self.__get_pandas_df_from_excel_sheet(
                sheet_name=sheet_name,
                skiprows=skiprows,
                skipfooter=skipfooter,
                cols_to_lowercase=cols_to_lowercase,
                **kwargs,
            )

    def reset_internal_dataframe(self, with_sheet_name: bool = False):
        """
        Resets the internal dataframe by re-reading data from the source. Can optionally
        include the sheet name during the data read operation based on the provided
        flag. The function utilizes the parameters `skiprows` and `skipfooter` that are
        predefined within the class.

        :param with_sheet_name: A boolean flag. If True, includes the sheet name during
            the data read operation. If False, sheet name is ignored.
        :type with_sheet_name: bool
        :return: None
        """
        assert with_sheet_name in [True, False], "with_sheet_name must be True or False"
        if with_sheet_name:
            self._read_data(skiprows=self.skiprows, skipfooter=self.skipfooter, sheet_name=self.sheet_name)
        else:
            self._read_data(skiprows=self.skiprows, skipfooter=self.skipfooter)

    def _read_data(self, sheet_name=None, skiprows=0, skipfooter=0, cols_to_lowercase=False, **kwargs):
        self._dataframe = self.__get_pandas_df_from_excel_sheet(
            sheet_name, skiprows, skipfooter, cols_to_lowercase, **kwargs
        )

    def __get_pandas_df_from_excel_sheet(
        self, sheet_name=None, skiprows=0, skipfooter=0, cols_to_lowercase=False, **kwargs
    ):
        df = pd.read_excel(
            self._original_file, sheet_name=sheet_name, skiprows=skiprows, skipfooter=skipfooter, **kwargs
        )

        if cols_to_lowercase and sheet_name is not None:
            return self._to_lowercase_columns(df)
        else:
            return df

    @property
    def columns(self) -> list:
        """Returns a list of column names for the current sheet or
        the column names for all sheets if the Excel file contains multiple sheets."""
        match self.dataframe:
            case pd.DataFrame():
                return list(self._dataframe.columns)
            case dict():
                return [{i: list(self._dataframe[i].columns)} for i in self._dataframe]
            case _:
                return []
