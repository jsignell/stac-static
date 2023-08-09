from pathlib import Path

import pystac
import pytest
import stac_geoparquet

here = Path(__file__).resolve().parent


@pytest.fixture(scope="module")
def test_case_1():
    URL = here / "data-files" / "test-case-1" / "catalog.json"
    catalog = pystac.read_file(URL)

    dicts = [item.to_dict() for item in catalog.get_items(recursive=True)]
    catalog._df = stac_geoparquet.to_geodataframe(dicts)
    return catalog
