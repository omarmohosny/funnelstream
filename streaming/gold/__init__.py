"""Gold reference builders: pure Spark functions over Silver DataFrames.

They are the reference implementations of the Databricks Gold build (fact_orders, fact_order_lines,
agg_orders_daily, agg_funnel_daily). Surrogate keys are looked up in optional dimension DataFrames and are -1
(the unknown member) without one. live_metrics_minute is LOCAL serving, not Gold.
"""
