"""Host tests for scripts/reset_scratch_gold.py: it empties the Gold tables of a scratch catalog and nothing else."""
import pytest

from scripts.reset_scratch_gold import REAL_CATALOG, reset_statements
from warehouse.tables import GOLD_TABLES


def test_every_gold_table_is_truncated_and_nothing_is_dropped():
    statements = reset_statements("workspace")
    assert statements == ["TRUNCATE TABLE workspace.gold.%s" % t.name for t in GOLD_TABLES]
    assert not any("DROP" in s.upper() for s in statements)


def test_only_the_gold_schema_is_touched():
    assert all(".gold." in s and ".silver." not in s for s in reset_statements("workspace"))


@pytest.mark.parametrize("catalog", [REAL_CATALOG, REAL_CATALOG.upper(), " funnelstream "])
def test_the_real_catalog_is_refused(catalog):
    with pytest.raises(ValueError, match="real catalog"):
        reset_statements(catalog)


def test_an_unsafe_catalog_name_is_refused():
    with pytest.raises(ValueError):
        reset_statements("workspace; DROP SCHEMA x")
