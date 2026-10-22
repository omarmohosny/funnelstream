"""apply_ddl adds the columns an existing Databricks table lacks (CREATE TABLE IF NOT EXISTS never does)."""
from scripts.apply_ddl import existing_columns, migrate_columns
from warehouse.tables import SILVER_MIRROR_TABLES


class FakeStore:
    def __init__(self, tables):
        self.tables, self.ran = tables, []

    def query(self, sql, params=None, retry=True):
        name = sql.split()[-1].split(".")[-1]
        cols = self.tables.get(name, [])
        if not cols:
            return []
        return [(c, "string", None) for c in cols] + [("", "", ""), ("# Partition Information", "", "")] \
            + [("event_date", "date", None)]

    def execute(self, sql, retry=True):
        self.ran.append(sql)


def _silver(name):
    return next(t for t in SILVER_MIRROR_TABLES if t.name == name)


def test_describe_rows_stop_at_the_first_blank_or_comment_row():
    t = _silver("silver_orders")
    assert existing_columns(FakeStore({"silver_orders": ["order_id", "order_total"]}), "c", t) == ["order_id", "order_total"]


def test_a_table_without_the_cairo_column_gets_it_added():
    t = _silver("silver_orders")
    old = [c.name for c in t.columns if c.name != "order_created_at_cairo"]
    store = FakeStore({"silver_orders": old})
    ran = migrate_columns(store, "workspace", [t])
    assert ran == ["ALTER TABLE workspace.silver.silver_orders ADD COLUMNS (order_created_at_cairo TIMESTAMP);"]
    assert store.ran == ran


def test_a_complete_table_and_a_missing_table_run_nothing():
    t = _silver("silver_events")
    assert migrate_columns(FakeStore({"silver_events": [c.name for c in t.columns]}), "c", [t]) == []
    assert migrate_columns(FakeStore({}), "c", [t]) == []  # absent: the CREATE statement makes it
