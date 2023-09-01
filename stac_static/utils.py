import functools
import json
from typing import Union

import geopandas
import pystac
import stac_geoparquet


@functools.singledispatch
def to_geodataframe(obj, **kwargs) -> geopandas.GeoDataFrame:
    raise TypeError


@to_geodataframe.register(pystac.Catalog)
@to_geodataframe.register(pystac.Collection)
def _(obj: Union[pystac.Catalog, pystac.Collection]) -> geopandas.GeoDataFrame:
    records = [item.to_dict() for item in obj.get_items(recursive=True)]
    return stac_geoparquet.to_geodataframe(records)


@to_geodataframe.register(pystac.ItemCollection)
def _(obj: pystac.ItemCollection):
    records = obj.to_dict()["features"]
    return stac_geoparquet.to_geodataframe(records)


def to_geoparquet(df: geopandas.GeoDataFrame, path: str) -> None:
    df2 = df.copy()
    df2.assets = df2.assets.apply(json.dumps)
    df2.to_parquet(path)
    return


def from_geoparquet(path: str) -> str:
    df = geopandas.read_parquet(path)
    df.assets = df.assets.apply(json.loads)
    return df
