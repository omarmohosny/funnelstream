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
