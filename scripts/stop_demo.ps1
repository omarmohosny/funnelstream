# Stops the pipeline container started by watch_demo.ps1 (Kafka, Postgres and Grafana keep running; Kafka keeps its messages).
docker stop fs-pipeline fs-bronze fs-silver fs-sync *> $null
docker ps --format '{{.Names}} {{.Status}}'
Write-Host "Pipeline stopped. Close the three windows. Kafka, Postgres and Grafana are still up (docker compose stop kafka postgres grafana to stop them)."
