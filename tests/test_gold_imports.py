"""Host smoke tests for the Gold reference package (no Spark session needed)."""


def test_gold_modules_import_and_mention_surrogate_keys():
    import streaming.gold as gold
    import streaming.gold.bots as bots
    import streaming.gold.build as build
    import streaming.gold.agg_orders as agg_orders
    import streaming.gold.funnel as funnel
    import streaming.gold.live as live
    import streaming.gold.orders as orders

    assert "surrogate" in (orders.__doc__ or "").lower()
    assert "surrogate" in orders.build_fact_orders.__doc__.lower()
    assert "surrogate" in orders.build_fact_order_lines.__doc__.lower()
    # agg_funnel_daily has no surrogate keys (C3): it documents that payment_method left the funnel instead
    assert "payment_method" in (funnel.__doc__ or "") and callable(funnel.build_agg_funnel_daily)
    assert callable(agg_orders.build_agg_orders_daily)
    assert callable(bots.mark_bot_sessions) and callable(live.refresh_live_metrics) and callable(build.build_all)
    assert gold.__doc__
