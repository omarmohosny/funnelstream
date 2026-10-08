# Stops the Bronze and Silver streaming containers started by watch_demo.ps1 (Kafka keeps running and keeps its messages).
docker stop fs-bronze fs-silver 2>&1 | Out-Null
docker ps --format '{{.Names}} {{.Status}}'
Write-Host "Bronze and Silver stopped. Close the four windows. Kafka is still up (docker compose stop kafka to stop it)."
