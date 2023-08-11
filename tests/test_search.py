import json

import pandas as pd
import pytest

from stac_static import search

GEOJSON = """{
  "type": "Polygon",
  "coordinates": [
    [
      [-3.04, 3.97],
      [-2, 3.97],
      [-2, 5],
      [-3.04, 5],
      [-3.04, 3.97]
    ]
  ]
}
"""


def test_ids(test_case_1):
    ids = "area-2-2-imagery,area-2-2-labels"
    result = search(test_case_1, ids=ids)
    assert result.matched() == 2


def test_collections(test_case_1):
    result = search(test_case_1, collections="area-1-1")
    df = result.as_geodataframe()
    assert len(df) == 2
    assert df.collection.unique().tolist() == ["area-1-1"]


def test_bbox(test_case_1):
    bbox = [-4, 3, -1, 4]
    result = search(test_case_1, bbox=bbox)
    assert result.matched() == 8


def test_intersects(test_case_1):
    intersects = GEOJSON
    result = search(test_case_1, intersects=intersects)
    assert result.matched() == 8


@pytest.mark.parametrize(
    "filter,n",
    [
        ("id LIKE '%labels'", 4),
        (
            {
                "op": "s_intersects",
                "args": [{"property": "geometry"}, json.loads(GEOJSON)],
            },
            8,
        ),
    ],
)
def test_filter(test_case_1, filter, n):
    result = search(test_case_1, filter=filter)
    assert result.matched() == n


@pytest.mark.parametrize(
    "value,start,end",
    [
        (
            "2022",
            pd.Timestamp("2022-01-01T00:00:00", tz="utc"),
            pd.Timestamp("2022-12-31T23:59:59.999999999", tz="utc"),
        ),
        ("../2022-03", None, pd.Timestamp("2022-03-31T23:59:59.999999999", tz="utc")),
        ("2022-03/..", pd.Timestamp("2022-03-01T00:00:00", tz="utc"), None),
    ],
)
def test_datetime_params(value, start, end, planet_disaster):
    result = search(planet_disaster, datetime=value)
    assert result._parameters["datetime"] == (start, end)
