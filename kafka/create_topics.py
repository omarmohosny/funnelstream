from pathlib import Path

import yaml

TOPICS = yaml.safe_load((Path(__file__).parent.parent / "contracts" / "topics.yaml").read_text())["topics"]


def create(bootstrap: str = "localhost:9092") -> None:
    from confluent_kafka.admin import AdminClient, NewTopic  # lazy import

    admin = AdminClient({"bootstrap.servers": bootstrap})
    new = [NewTopic(name, num_partitions=cfg["partitions"], replication_factor=1,
                    config={"retention.ms": str(cfg["retention_hours"] * 3600 * 1000)})
           for name, cfg in TOPICS.items()]
    for name, fut in admin.create_topics(new).items():
        try:
            fut.result()
            print("created", name)
        except Exception as e:
            print("skip", name, e)


if __name__ == "__main__":
    create()
