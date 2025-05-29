import fiona
import geopandas as gpd

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class GeoPackageDataReader(BaseDataReader):
    def __init__(self, input_source, encoding="utf-8", layer=None):
        super().__init__(input_source)
        self._layer = layer
        self._layers = None
        self._encoding = encoding

    def _read_data(self, layer, encoding="utf-8"):
        self._dataframe = gpd.read_file(self._input_source, layer=layer, encoding=layer)

    def read_layer(self, layer, encoding="utf-8") -> gpd.GeoDataFrame:
        self._layer = layer
        self._encoding = encoding
        self._read_data(layer, encoding)
        return self._dataframe

    @property
    def dataframe(self) -> gpd.GeoDataFrame:
        if self._dataframe is None:
            self._read_data(self._layer, self._encoding)
        return self._dataframe

    @property
    def layers(self):
        if self._layers is None:
            self._layers = fiona.listlayers(self._input_source)
        return self._layers
