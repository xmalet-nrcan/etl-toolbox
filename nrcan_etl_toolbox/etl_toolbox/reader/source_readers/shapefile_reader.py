import geopandas as gpd

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class ShapefileReader(BaseDataReader):
    """
    Classe pour lire un fichier Shapefile et en extraire un GeoDataFrame.
    """

    def _read_data(self, **kwargs):
        # Utilise GeoPandas pour lire un fichier de type shapefile
        self._dataframe = gpd.read_file(self._input_source, driver="ESRI Shapefile", **kwargs)
