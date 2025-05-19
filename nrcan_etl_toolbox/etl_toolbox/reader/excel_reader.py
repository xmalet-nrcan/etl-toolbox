import pandas as pd

from nrcan_etl_toolbox.etl_toolbox.reader.base_reader import BaseDataReader


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
        self._original_file = pd.ExcelFile(self._input_source)
        self.sheet_name = sheet_name

    def get_sheet_names(self):
        return list(self._original_file.sheet_names)

    def read_sheet(
        self, sheet_name, set_internal_dataframe: bool = False, skiprows=0, skipfooter=0, **kwargs
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
        if sheet_name not in self.get_sheet_names():
            raise ValueError(f"Sheet {sheet_name} not found in Excel file.")
        self.sheet_name = sheet_name
        if set_internal_dataframe:
            self._read_data(sheet_name=sheet_name, skiprows=skiprows, skipfooter=skipfooter, **kwargs)
            return self._dataframe
        else:
            self._read_data(skiprows=skiprows, skipfooter=skipfooter, **kwargs)
            return pd.read_excel(
                self._original_file, sheet_name=sheet_name, skiprows=skiprows, skipfooter=skipfooter, **kwargs
            )

    def _read_data(self, sheet_name=None, skiprows=0, skipfooter=0, **kwargs):
        self._dataframe = pd.read_excel(
            self._original_file, sheet_name=sheet_name, skiprows=skiprows, skipfooter=skipfooter, **kwargs
        )

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

