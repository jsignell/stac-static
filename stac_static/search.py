import json
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
from pygeofilter.backends.geopandas.evaluate import to_filter
from pygeofilter.parsers import cql2_json, cql2_text


class Parsers(Enum):
    cql2_json = cql2_json.parse
    cql2_text = cql2_text.parse


class GeoInterface(Protocol):
    @property
    def __geo_interface__(self) -> Dict[str, Any]:
        ...


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


def search(catalog, **params):
    return ItemSearch(catalog, **params)


def _search(df, **params):
    subset = df.copy()

    if "ids" in params:
        params["ids"]
        subset = subset.query("id in @ids")

    if "collections" in params:
        params["collections"]
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


def to_geoparquet(catalog: pystac.Catalog):
    import stac_geoparquet

    dicts = [item.to_dict() for item in catalog.get_items(recursive=True)]
    df = stac_geoparquet.to_geodataframe(dicts)

    df.to_parquet(f"{catalog.id}.parq")


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
        catalog : pystac.Catalog object
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
        catalog: pystac.Catalog,
        *,
        ids: Optional[ListLike] = None,
        collections: Optional[ListLike] = None,
        bbox: Optional[BBoxLike] = None,
        intersects: Optional[IntersectsLike] = None,
        filter: Optional[FilterLike] = None,
        filter_lang: Optional[FilterLangLike] = None,
    ):
        self.catalog = catalog

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

    def as_dataframe(self):
        return _search(self.catalog._df, **self._parameters)

    def matched(self):
        return len(_search(self.catalog._df, **self._parameters))
