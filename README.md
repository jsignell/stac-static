# stac-static

Implements STAC search for static catalogs following the pystac-client API.

```python
import pystac
import dask_geopandas
import stac_geoparquet

URL = "https://storage.googleapis.com/cfo-public/catalog.json"
catalog = pystac.read_file(URL)

dicts = [item.to_dict() for item in catalog.get_items(recursive=True)]
df = stac_geoparquet.to_geodataframe(dicts)
df.to_parquet("cfo.parq")

ddf = dask_geopandas.from_geopandas(df, npartitions=1)
ddf.to_parquet("cfo-dd.parq", partition_on=["collection"])


```

Ref:

- https://eodag.readthedocs.io/en/stable/notebooks/tutos/tuto_stac_client.html#STAC-Static-catalog

- https://github.com/CS-SI/eodag/blob/f2ac36ba14d3b9b6a09daf0b0b7323dca59766ac/eodag/plugins/search/static_stac_search.py#L34
