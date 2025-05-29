import pandas as pd

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class JSONReader(BaseDataReader):
    """
    Classe pour lire un fichier JSON et en extraire un DataFrame.
    """

    def _read_data(self, **kwargs):
        # Utilise pandas pour lire un fichier JSON
        self._dataframe = pd.read_json(self._input_source, **kwargs)
