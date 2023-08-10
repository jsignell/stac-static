import json
from functools import cached_property, cache
from copy import deepcopy
from datetime import datetime as datetime_
from enum import Enum
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Protocol,
    Tuple,
    Union,
)

import numpy as np
import pystac
import shapely
import geopandas
from pygeofilter.backends.geopandas.evaluate import to_filter
from pygeofilter.parsers import cql2_json, cql2_text
from stac_static.utils import catalog_to_geodataframe
import stac_geoparquet

class Parsers(Enum):
    cql2_json = cql2_json.parse
    cql2_text = cql2_text.parse


class GeoInterface(Protocol):
    @property
    def __geo_interface__(self) -> Dict[str, Any]:
        ...


CatalogLike = Union[pystac.Catalog, geopandas.GeoDataFrame]

DatetimeOrTimestamp = Optional[Union[datetime_, str]]
Datetime = str
DatetimeLike = Union[
    DatetimeOrTimestamp,
    Tuple[DatetimeOrTimestamp, DatetimeOrTimestamp],
    List[DatetimeOrTimestamp],
    Iterator[DatetimeOrTimestamp],
]

BBox = Tuple[float, ...]
BBoxLike = Union[BBox, List[float], Iterator[float], str]

ListLike = Union[List[str], Iterator[str], str]

Intersects = Dict[str, Any]
IntersectsLike = Union[str, GeoInterface, Intersects]

FilterLangLike = str
FilterLike = Union[Dict[str, Any], str]


def search(catalog: CatalogLike, **params):
    return ItemSearch(catalog, **params)


def _search(df: geopandas.GeoDataFrame, **params):
    subset = df.copy()

    if "ids" in params:
        ids = params["ids"]
        subset = subset.query("id in @ids")

    if "collections" in params:
        collections = params["collections"]
        subset = subset.query("collection in @collections")

    if "bbox" in params:
        bbox = params["bbox"]
        bbox_shape = shapely.geometry.box(*bbox)
        subset = subset[subset.geometry.intersects(bbox_shape)]

    if "intersects" in params:
        intersects = params["intersects"]
        intersects_shape = shapely.geometry.shape(intersects)
        subset = subset[subset.geometry.intersects(intersects_shape)]

    if "filter" in params:
        parse = getattr(Parsers, params["filter-lang"].replace("-", "_"))
        ast = parse(params["filter"])
        subset = subset[to_filter(subset, ast, {}, {"sin": np.sin})]

    return subset


class ItemSearch:
    """Represents a deferred query to a static STAC catalog that mimics
    `STAC API - Item Search spec
    <https://github.com/radiantearth/stac-api-spec/tree/master/item-search>`__.

    All parameters except ``catalog`` correspond to query parameters
    described in the `STAC API - Item Search: Query Parameters Table
    <https://github.com/radiantearth/stac-api-spec/tree/master/item-search#query-parameter-table>`__
    docs. Please refer
    to those docs for details on how these parameters filter search results.

    Args:
        catalog : pystac.Catalog object or geopandas.GeoDataFrame representation of STAC items
        ids: List of one or more Item ids to filter on.
        collections: List of one or more Collection IDs or :class:`pystac.Collection`
            instances. Only Items in one
            of the provided Collections will be searched
        bbox: A list, tuple, or iterator representing a bounding box of 2D
            or 3D coordinates. Results will be filtered
            to only those intersecting the bounding box.
        intersects: A string or dictionary representing a GeoJSON geometry, or
            an object that implements a
            ``__geo_interface__`` property, as supported by several libraries
            including Shapely, ArcPy, PySAL, and
            geojson. Results filtered to only those intersecting the geometry.
        filter: JSON of query parameters as per the STAC API `filter` extension
        filter_lang: Language variant used in the filter body. If `filter` is a
            dictionary or not provided, defaults
            to 'cql2-json'. If `filter` is a string, defaults to `cql2-text`.
    """

    def __init__(
        self,
        catalog: CatalogLike,
        *,
        ids: Optional[ListLike] = None,
        collections: Optional[ListLike] = None,
        bbox: Optional[BBoxLike] = None,
        intersects: Optional[IntersectsLike] = None,
        filter: Optional[FilterLike] = None,
        filter_lang: Optional[FilterLangLike] = None,
    ):
        if isinstance(catalog, pystac.Catalog):
            self.df = catalog_to_geodataframe(catalog)
        else:
            self.df = catalog

        params = {
            "bbox": self._format_bbox(bbox),
            "ids": self._format_listlike(ids),
            "collections": self._format_listlike(collections),
            "intersects": self._format_intersects(intersects),
            "filter": filter,
            "filter-lang": self._format_filter_lang(filter, filter_lang),
        }

        self._parameters: Dict[str, Any] = {
            k: v for k, v in params.items() if v is not None
        }

    @staticmethod
    def _format_listlike(value: Optional[ListLike]) -> Optional[Tuple[str, ...]]:
        if value is None:
            return None

        if isinstance(value, str):
            return tuple(value.split(","))

        return tuple(value)

    @staticmethod
    def _format_filter_lang(
        _filter: Optional[FilterLike], value: Optional[FilterLangLike]
    ) -> Optional[str]:
        if _filter is None:
            return None

        if value is not None:
            return value

        if isinstance(_filter, str):
            return "cql2-text"

        if isinstance(_filter, dict):
            return "cql2-json"

        return None

    @staticmethod
    def _format_intersects(value: Optional[IntersectsLike]) -> Optional[Intersects]:
        if value is None:
            return None
        if isinstance(value, dict):
            return deepcopy(value)
        if isinstance(value, str):
            return dict(json.loads(value))
        if hasattr(value, "__geo_interface__"):
            return dict(deepcopy(getattr(value, "__geo_interface__")))
        raise Exception(
            "intersects must be of type None, str, dict, or an object that "
            "implements __geo_interface__"
        )

    @staticmethod
    def _format_bbox(value: Optional[BBoxLike]) -> Optional[BBox]:
        if value is None:
            return None

        if isinstance(value, str):
            bbox = tuple(map(float, value.split(",")))
        else:
            bbox = tuple(map(float, value))

        return bbox

    @property
    def parameters(self):
        """Read-only view of parameters"""
        return self._parameters.copy()

    @cached_property
    def result(self) -> geopandas.GeoDataFrame:
        return _search(self.df, **self.parameters)

    def as_geodataframe(self):
        return self.result

    def matched(self):
        """Number matched for search

        NOTE: Unlike pystac-client this will trigger the search

        Returns:
            int: Total count of matched items.
        """
        return len(self.result)

    @cache
    def item_collection(self) -> pystac.ItemCollection:
        """
        Get the matching items as a :py:class:`pystac.ItemCollection`.

        Return:
            ItemCollection: The item collection
        """
        return stac_geoparquet.to_item_collection(self.result)

    def items(self) -> Iterator[pystac.Item]:
        """Iterator that yields :class:`pystac.Item` instances for each item matching
        the given search parameters.

        Yields:
            Item : each Item matching the search criteria
        """
        for item in self.item_collection():
            yield item

    def items_as_dicts(self) -> Iterator[Dict[str, Any]]:
        """Iterator that yields :class:`dict` instances for each item matching
        the given search parameters.

        Yields:
            Item : each Item matching the search criteria
        """
        for item in self.items():
            yield item.to_dict()
