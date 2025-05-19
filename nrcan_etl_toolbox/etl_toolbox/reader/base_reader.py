from abc import abstractmethod

import pandas as pd


class BaseDataReader:
    def __init__(self, input_source):
        self._input_source = input_source
        self._dataframe: pd.DataFrame = None

    @abstractmethod
    def _read_data(self, **kwargs):
        """Read data from the input source and set self._dataframe content"""
        pass

    @property
    def dataframe(self) -> pd.DataFrame:
        if self._dataframe is None:
            self._read_data()
        return self._dataframe

    @property
    def columns(self) -> list:
        return self.dataframe.columns.tolist()
