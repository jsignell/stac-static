# stac-static

Implements search for static STAC catalogs following the pystac-client API.

There are a whole bunch of static STAC catalogs listed at:
https://stacindex.org/catalogs?access=public&type=static

## Example

```python
import pystac
from stac_static import search

URL = "https://storage.googleapis.com/cfo-public/catalog.json"
catalog = pystac.read_file(URL)

result = search(catalog, collections="vegetation")
result.item_collection()
```

## How it works

Right now this requires that there be a geodataframe representation of the
catalog following the `stac_geoparquet` convention. I am still working out
how the data model should work. For now you can pass a catalog or you can
pass the df directly:

```python
import pystac
from stac_static import search, utils

URL = "https://storage.googleapis.com/cfo-public/catalog.json"
catalog = pystac.read_file(URL)

df = utils.catalog_to_geodataframe(catalog)
result = search(df, collections="vegetation")
result.item_collection()
```

## What doesn't work yet

- datetime
- filtering with temporal filters.
   - This work depends on [pygeofilter](https://github.com/geopython/pygeofilter) which has not yet implemented temporal filters.

## Prior Art

EODAG has static catalog search: [docs](https://eodag.readthedocs.io/en/stable/notebooks/tutos/tuto_stac_client.html#STAC-Static-catalog) [code](https://github.com/CS-SI/eodag/blob/f2ac36ba14d3b9b6a09daf0b0b7323dca59766ac/eodag/plugins/search/static_stac_search.py#L34)
