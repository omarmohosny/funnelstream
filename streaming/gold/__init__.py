"""Gold reference builders: pure Spark functions over Silver DataFrames.

They are the reference implementations of the Databricks Gold build (Task 10c turns them into MERGE
SQL and adds the surrogate keys and the attribution campaign_id). live_metrics_minute is LOCAL serving,
not Gold.
"""
