import pathlib

import geopandas as gpd

from nrcan_etl_toolbox.etl_toolbox.reader.source_readers.base_reader import BaseDataReader


class GeoPackageDataReader(BaseDataReader):
    def __init__(self, input_source, encoding="utf-8", layer=None):
        super().__init__(input_source)
        self._layer = layer
        self._layers = None
        self._encoding = encoding
        assert pathlib.Path(input_source).exists(), "Input GeoPackage file does not exist."

    def _read_data(self, layer, encoding="utf-8"):
        if layer is None:
            raise ValueError("Layer name must be provided to read data from a GeoPackage.")
        if layer not in self.layers :
            raise ValueError(f"Layer '{layer}' not found in the GeoPackage. Available layers: {self.layers}")
        if len(self.layers) == 0:
            raise ValueError(f"GeoPackage {self._input_source} has no feature layers to read.")
        self._dataframe = gpd.read_file(self._input_source, layer=layer, encoding=encoding)

    def read_layer(self, layer, encoding="utf-8") -> gpd.GeoDataFrame:
        self._layer = layer
        self._encoding = encoding
        self._read_data(layer, encoding)
        return self._dataframe   # noqa:

    @property
    def dataframe(self) -> gpd.GeoDataFrame:
        if self._dataframe is None:
            self._read_data(self._layer, self._encoding)
        return self._dataframe # noqa

    @property
    def layers(self):
        if self._layers is None:
            try:
                import sqlite3
            except ImportError:
                raise ImportError("sqlite3 module is required to read layers from a GeoPackage file.")
            with sqlite3.connect(self._input_source) as conn:
                cursor = conn.execute("""
                                      SELECT table_name
                                      FROM gpkg_contents
                                          where data_type = 'features'
                                      ORDER BY table_name;
                                      """)
                self._layers = [row[0] for row in cursor.fetchall()]
        return self._layers

