import json
from collections.abc import Iterator
from copy import deepcopy
from datetime import datetime as datetime_
from enum import Enum
from functools import cache, cached_property
from typing import (
    Any,
    Optional,
    Protocol,
    Union,
)

import geopandas
import numpy as np
import pandas as pd
import pystac
import shapely
import stac_geoparquet
from pygeofilter.backends.geopandas.evaluate import to_filter
from pygeofilter.parsers import cql2_json, cql2_text

from stac_static.utils import to_geodataframe


class Parsers(Enum):
    cql2_json = cql2_json.parse
    cql2_text = cql2_text.parse


class GeoInterface(Protocol):
    @property
    def __geo_interface__(self) -> dict[str, Any]:
        ...


CatalogLike = Union[pystac.Catalog, geopandas.GeoDataFrame]

DatetimeOrTimestamp = Optional[Union[datetime_, str, pd.Timestamp]]
Datetime = tuple[pd.Timestamp, pd.Timestamp]
DatetimeLike = Union[
    DatetimeOrTimestamp,
    tuple[DatetimeOrTimestamp, DatetimeOrTimestamp],
    list[DatetimeOrTimestamp],
    Iterator[DatetimeOrTimestamp],
]

BBox = tuple[float, ...]
BBoxLike = Union[BBox, list[float], Iterator[float], str]

ListLike = Union[list[str], Iterator[str], str]

Intersects = dict[str, Any]
IntersectsLike = Union[str, GeoInterface, Intersects]

FilterLangLike = str
FilterLike = Union[dict[str, Any], str]


def search(catalog: CatalogLike, **params):
    return ItemSearch(catalog, **params)


def _search(df: geopandas.GeoDataFrame, **params):
    subset = df.copy()

    if "ids" in params:
        ids = params["ids"]
        subset = subset[subset["id"].isin(ids)]

    if "collections" in params:
        collections = params["collections"]
        subset = subset[subset["collection"].isin(collections)]

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

    if "datetime" in params:
        start, end = params["datetime"]
        if start is not None:
            subset = subset[subset.datetime >= start]
        if end is not None:
            subset = subset[subset.datetime <= end]

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
        catalog : pystac.Catalog object or geopandas.GeoDataFrame representation of STAC
            items
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
        datetime: Either a single datetime or datetime range used to filter results.
            You may express a single datetime using a :class:`datetime.datetime`
            instance, a `RFC 3339-compliant <https://tools.ietf.org/html/rfc3339>`__
            timestamp, or a simple date string (see below). Instances of
            :class:`datetime.datetime` may be either
            timezone aware or unaware. Timezone aware instances will be converted to
            a UTC timestamp before being passed
            to the endpoint. Timezone unaware instances are assumed to represent UTC
            timestamps. You may represent a
            datetime range using a ``"/"`` separated string as described in the spec,
            or a list, tuple, or iterator
            of 2 timestamps or datetime instances. For open-ended ranges, use either
            ``".."`` (``'2020-01-01:00:00:00Z/..'``,
            ``['2020-01-01:00:00:00Z', '..']``) or a value of ``None``
            (``['2020-01-01:00:00:00Z', None]``).

            If using a simple date string, the datetime can be specified in
            ``YYYY-mm-dd`` format, optionally truncating
            to ``YYYY-mm`` or just ``YYYY``. Simple date strings will be expanded to
            include the entire time period, for example:

            - ``2017`` expands to ``2017-01-01T00:00:00Z/2017-12-31T23:59:59Z``
            - ``2017-06`` expands to ``2017-06-01T00:00:00Z/2017-06-30T23:59:59Z``
            - ``2017-06-10`` expands to ``2017-06-10T00:00:00Z/2017-06-10T23:59:59Z``

            If used in a range, the end of the range expands to the end of that
            day/month/year, for example:

            - ``2017/2018`` expands to
              ``2017-01-01T00:00:00Z/2018-12-31T23:59:59Z``
            - ``2017-06/2017-07`` expands to
              ``2017-06-01T00:00:00Z/2017-07-31T23:59:59Z``
            - ``2017-06-10/2017-06-11`` expands to
              ``2017-06-10T00:00:00Z/2017-06-11T23:59:59Z``

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
        datetime: Optional[DatetimeLike] = None,
        filter: Optional[FilterLike] = None,
        filter_lang: Optional[FilterLangLike] = None,
    ):
        if isinstance(
            catalog, (pystac.Catalog, pystac.Collection, pystac.ItemCollection)
        ):
            self.df = to_geodataframe(catalog)
        else:
            self.df = catalog

        params = {
            "bbox": self._format_bbox(bbox),
            "datetime": self._format_datetime(datetime),
            "ids": self._format_listlike(ids),
            "collections": self._format_listlike(collections),
            "intersects": self._format_intersects(intersects),
            "filter": filter,
            "filter-lang": self._format_filter_lang(filter, filter_lang),
        }

        self._parameters: dict[str, Any] = {
            k: v for k, v in params.items() if v is not None
        }

    def _format_datetime(self, value: Optional[DatetimeLike]) -> Optional[Datetime]:
        """Convert input to a tuple of start and end pd.Timestamps.

        None in the output tuple means that bound is unset.
        """
        if value is None:
            return None
        elif isinstance(value, str):
            components = value.split("/")
        else:
            components = list(value)  # type: ignore

        components = [c for c in components if c is not None]
        if not components:
            return None
        elif len(components) == 1:
            period = pd.Period(components[0])
            start = period.start_time.tz_localize("utc")
            end = period.end_time.tz_localize("utc")
            return start, end
        elif len(components) == 2:
            if components[0] in ["..", None]:
                start = None
            else:
                start = pd.Period(components[0]).start_time.tz_localize("utc")
            if components[1] in ["..", None]:
                end = None
            else:
                end = pd.Period(components[1]).end_time.tz_localize("utc")
            return start, end
        else:
            raise Exception(
                "too many datetime components "
                f"(max=2, actual={len(components)}): {value}"
            )

    @staticmethod
    def _format_listlike(value: Optional[ListLike]) -> Optional[tuple[str, ...]]:
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
        yield from self.item_collection()

    def items_as_dicts(self) -> Iterator[dict[str, Any]]:
        """Iterator that yields :class:`dict` instances for each item matching
        the given search parameters.

        Yields:
            Item : each Item matching the search criteria
        """
        for item in self.items():
            yield item.to_dict()
