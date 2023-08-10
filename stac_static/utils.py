import stac_geoparquet
import geopandas
import pystac


def catalog_to_geodataframe(catalog: pystac.Catalog) -> geopandas.GeoDataFrame:
    dicts = [item.to_dict() for item in catalog.get_items(recursive=True)]
    return stac_geoparquet.to_geodataframe(dicts)
