import pytest


@pytest.fixture(scope="module")
def spark():
    from streaming.common.spark import get_spark

    session = get_spark("funnelstream-tests")
    yield session
    session.stop()


def pytest_collection_modifyitems(config, items):
    import os

    if os.name == "nt" and os.environ.get("RUN_SPARK_TESTS") != "1":
        skip = pytest.mark.skip(reason="Spark needs winutils on Windows; run in Docker")
        for item in items:
            if "spark" in item.keywords:
                item.add_marker(skip)
