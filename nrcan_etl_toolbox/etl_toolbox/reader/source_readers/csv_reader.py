import pandas as pd

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class CSVReader(BaseDataReader):
    """
    Classe pour lire un fichier CSV et en extraire un DataFrame.
    """

    def __init__(self, input_source,
                 encoding="utf-8",
                 delimiter=',',
                 skiprows=0,
                 skipfooter=0,
                 nrows=None,
                 cols_to_lowercase=True,
                 pandas_read_csv_kwargs=None,
                 **kwargs):
        super().__init__(input_source)
        if pandas_read_csv_kwargs is None:
            pandas_read_csv_kwargs = {}
        self._skiprows= skiprows
        self._skipfooter = skipfooter
        self._delimiter = delimiter
        self._encoding = encoding
        self._nrows = nrows
        self._pandas_read_csv_kwargs = pandas_read_csv_kwargs
        self._kwargs = kwargs
        self._cols_to_lowercase = cols_to_lowercase

    def _read_data(self, **kwargs):
        # Utilise pandas pour lire le fichier CSV
        self._dataframe = pd.read_csv(self._input_source,
                                      delimiter=self._delimiter,
                                      skiprows=self._skiprows,
                                      skipfooter=self._skipfooter,
                                      nrows=self._nrows,
                                      **self._pandas_read_csv_kwargs,
                                      **self._kwargs, **kwargs)
        if self._cols_to_lowercase:
            self._dataframe = self._to_lowercase_columns(self._dataframe)
