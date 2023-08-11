from pathlib import Path

import pystac
import pytest

from stac_static.utils import to_geodataframe

here = Path(__file__).resolve().parent


@pytest.fixture(scope="module", params=[lambda x: x, to_geodataframe])
def test_case_1(request):
    """test_case_1 two ways: as a catalog and as a geodataframe"""
    URL = here / "data-files" / "test-case-1" / "catalog.json"
    return request.param(pystac.read_file(URL))


@pytest.fixture(scope="module")
def planet_disaster():
    """planet_disaster as a catalog"""
    URL = here / "data-files" / "planet-disaster" / "collection.json"
    return pystac.read_file(URL)
