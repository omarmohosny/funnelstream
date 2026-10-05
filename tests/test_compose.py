import yaml
from pathlib import Path

COMPOSE = Path("docker-compose.yml")
REQUIRED = {"kafka", "postgres", "airflow"}


def load():
    return yaml.safe_load(COMPOSE.read_text())


def test_required_services_exist():
    services = load()["services"]
    assert REQUIRED.issubset(services.keys())


def test_every_service_has_memory_limit():
    services = load()["services"]
    for name, svc in services.items():
        assert "mem_limit" in svc, f"{name} has no mem_limit"


def test_postgres_is_16():
    assert load()["services"]["postgres"]["image"].startswith("postgres:16")


def test_minio_is_not_a_service():
    services = load()["services"]
    assert "minio" not in services


def test_kafka_advertised_listeners():
    """Kafka must advertise localhost:9092 for host clients and kafka:19092 for containers."""
    kafka = load()["services"]["kafka"]
    advertised = kafka["environment"]["KAFKA_ADVERTISED_LISTENERS"]
    assert "EXTERNAL://localhost:9092" in advertised
    assert "INTERNAL://kafka:19092" in advertised


def test_airflow_volume_mounts():
    """Airflow must mount warehouse, contracts, and data directories."""
    airflow = load()["services"]["airflow"]
    volumes = airflow["volumes"]
    mounted_paths = [v.split(":")[1] if ":" in v else v for v in volumes]
    assert "/opt/airflow/warehouse" in mounted_paths
    assert "/opt/airflow/contracts" in mounted_paths
    assert "/opt/airflow/data" in mounted_paths


def test_airflow_databricks_requirements():
    """Airflow must have _PIP_ADDITIONAL_REQUIREMENTS with databricks-sql-connector, requests, pyarrow."""
    airflow = load()["services"]["airflow"]
    env = airflow["environment"]
    # env can be a dict or list of KEY=VALUE strings
    pip_reqs = None
    if isinstance(env, dict):
        pip_reqs = env.get("_PIP_ADDITIONAL_REQUIREMENTS", "")
    else:
        for item in env:
            if item.startswith("_PIP_ADDITIONAL_REQUIREMENTS="):
                pip_reqs = item.split("=", 1)[1]
                break
    assert pip_reqs is not None, "_PIP_ADDITIONAL_REQUIREMENTS not found"
    assert "databricks-sql-connector" in pip_reqs
    assert "requests" in pip_reqs
    assert "pyarrow" in pip_reqs


def test_airflow_ad_api_url():
    """Airflow must have AD_API_URL pointing to http://host.docker.internal:8000."""
    airflow = load()["services"]["airflow"]
    env = airflow["environment"]
    ad_url = None
    if isinstance(env, dict):
        ad_url = env.get("AD_API_URL")
    else:
        for item in env:
            if item.startswith("AD_API_URL="):
                ad_url = item.split("=", 1)[1]
                break
    assert ad_url == "http://host.docker.internal:8000"


def test_airflow_extra_hosts():
    """Airflow must have extra_hosts for host.docker.internal."""
    airflow = load()["services"]["airflow"]
    assert "extra_hosts" in airflow
    extra_hosts = airflow["extra_hosts"]
    assert "host.docker.internal:host-gateway" in extra_hosts


def test_env_example_has_required_names():
    """env.example must contain the new environment variable names (no secrets)."""
    env_path = Path(".env.example")
    content = env_path.read_text()
    required_names = [
        "PG_DSN",
        "AD_API_URL",
        "SIM_SEED",
        "DATABRICKS_CATALOG",
        "DATABRICKS_VOLUME",
    ]
    for name in required_names:
        assert name in content, f"{name} not in .env.example"


def test_env_example_no_real_tokens():
    """env.example must not contain real tokens (DATABRICKS_TOKEN= should be empty)."""
    env_path = Path(".env.example")
    content = env_path.read_text()
    lines = content.split("\n")
    for line in lines:
        if line.startswith("DATABRICKS_TOKEN="):
            value = line.split("=", 1)[1].strip()
            assert value == "" or value.startswith("change"), f"DATABRICKS_TOKEN has a value: {value}"


def test_spark_service():
    svc = load()["services"]["spark"]
    assert svc["build"] == "infrastructure/docker/spark"
    assert svc["profiles"] == ["spark"]
    assert svc["mem_limit"]
    assert "./:/app" in svc["volumes"]
    assert "ivycache" in load()["volumes"]


def test_spark_env_file_is_optional_dotenv():
    """DATABRICKS_* and GOLD_BACKEND reach the spark container when .env exists; compose works without it."""
    env_file = load()["services"]["spark"]["env_file"]
    assert isinstance(env_file, list)
    entries = [e if isinstance(e, dict) else {"path": e, "required": True} for e in env_file]
    assert {"path": ".env", "required": False} in entries
