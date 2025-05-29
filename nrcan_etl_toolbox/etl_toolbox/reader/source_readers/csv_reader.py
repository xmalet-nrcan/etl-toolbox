import pandas as pd

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class CSVReader(BaseDataReader):
    """
    Classe pour lire un fichier CSV et en extraire un DataFrame.
    """

    def _read_data(self, **kwargs):
        # Utilise pandas pour lire le fichier CSV
        self._dataframe = pd.read_csv(self._input_source, **kwargs)
