# stac-static

Implements search for static STAC catalogs following the pystac-client API.

There are a whole bunch of static STAC catalogs listed at:
https://stacindex.org/catalogs?access=public&type=static

## Example

Pass a catalog object into the `search` function (slow)

```python
import pystac
from stac_static import search

URL = "https://www.planet.com/data/stac/disasters/hurricane-harvey/catalog.json"
catalog = pystac.read_file(URL)

result = search(catalog, filter="platform = 'SS02'")
result.item_collection()
```

Or pass a geodataframe representing some items (faster)

```python
import geopandas
import pystac
import planetary_computer as pc
from stac_static import search

URL = "https://planetarycomputer.microsoft.com/api/stac/v1/collections/io-lulc-9-class"
collection = pystac.read_file(URL)

asset = pc.sign(collection.assets["geoparquet-items"])

df = geopandas.read_parquet(
   asset.href, storage_options=asset.extra_fields["table:storage_options"]
)
result = search(df, datetime="2019")
list(result.items())
```

If you are going to be searching the same catalog repeatedly and there is no
predefined geoparquet file to download, then you can create one and cache it
locally

```python
import geopandas
import pystac
from stac_static import search, from_geoparquet, to_geoparquet, to_geodataframe

URL = "https://www.planet.com/data/stac/disasters/hurricane-harvey/catalog.json"
catalog = pystac.read_file(URL)
path = f"{catalog.id}.parq"

df = to_geodataframe(catalog)
to_geoparquet(df, path)
```

Once you have cached the geodataframe you can search on that (fastest)

```python
df = from_geoparquet(cache_path)

result = search(df, filter="view:azimuth < 200")
result.item_collection()
```

## What doesn't work yet

- filtering with temporal filters.
   - This work depends on [pygeofilter](https://github.com/geopython/pygeofilter) which has not yet implemented temporal filters.

- query: no plan to support this since it is redundant with filters

## Prior Art

EODAG has static catalog search: [docs](https://eodag.readthedocs.io/en/stable/notebooks/tutos/tuto_stac_client.html#STAC-Static-catalog) [code](https://github.com/CS-SI/eodag/blob/f2ac36ba14d3b9b6a09daf0b0b7323dca59766ac/eodag/plugins/search/static_stac_search.py#L34)
