<#
Opens three windows so you can watch data move through every stage, live:
  1. Kafka listener   : every message as it arrives on app.events, orders, delivery.events
  2. Pipeline         : ONE Spark job (Bronze ingest + Silver clean), [bronze] progress lines and [silver] batch views;
                        the Silver job also fills the live and health tables in Postgres
  3. Generator        : the simulator in live mode, printing the events it publishes
Postgres and Grafana are started first (Grafana: http://localhost:3000).
Run from anywhere:  .\scripts\watch_demo.ps1   [-Events 3000 (per round)] [-Once] [-Rate 50] [-Seed N] [-ShowEvery 5] [-Sample 3] [-Inventory]
Stop:               .\scripts\stop_demo.ps1   (then close the windows)
#>
param(
    [int]$Events = 3000,
    [int]$Rate = 50,
    [int]$Seed = 0,          # 0 = next unused seed (kept in data\state\next_seed.txt), so ids never repeat
    [int]$UniverseSeed = 7,  # same customers and products on every run
    [int]$ShowEvery = 5,     # generator prints every Nth event
    [int]$Sample = 3,        # Bronze and Silver print the newest N rows per batch
    [int]$StartDelay = 75,   # seconds Spark gets to start before the generator begins
    [int]$SyncEvery = 300,   # seconds between Silver -> Databricks -> Gold rounds; 0 = no Gold sync window
    [switch]$Inventory,      # also publish simulated stock events and run the hero stock-out scenario
    [switch]$Once            # stop after -Events events; without it the generator runs until Ctrl+C
)
$ErrorActionPreference = 'Continue'   # docker writes progress to stderr; Stop would turn that into a fatal error in PowerShell 5.1. Failures are caught by the exit-code checks below.
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Fail($m) { Write-Host "FAILED: $m" -ForegroundColor Red; exit 1 }

# Docker up?
docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker Desktop is not running. Start it, wait a minute, run this again." }
if (-not (Test-Path .venv\Scripts\python.exe)) { Fail ".venv is missing (python -m venv .venv; .venv\Scripts\python -m pip install -r requirements.txt)" }

# memory: Kafka 1 GB + Postgres 0.5 GB + Grafana 0.5 GB + ONE Spark container 3 GB
docker rm -f fs-pipeline fs-bronze fs-silver *> $null

# Kafka + topics
docker compose up -d kafka *> $null
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list *> $null
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
    Start-Sleep 3
}
if (-not $ready) { Fail "Kafka did not become ready" }
.venv\Scripts\python kafka/create_topics.py | Out-Null

# Postgres (live and health tables) + the DDL, then Grafana
docker compose up -d postgres *> $null
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    docker compose exec -T postgres sh -c 'pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"' *> $null
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
    Start-Sleep 2
}
if (-not $ready) { Fail "Postgres did not become ready" }
docker compose run --rm spark python scripts/apply_live_ddl.py
if ($LASTEXITCODE -eq 3) {
    Write-Host "WARNING: GRAFANA_DB_PASSWORD is missing from .env. The live tables exist but the Grafana role grafana_ro was NOT created, so Grafana cannot read them. Set it in .env and run: docker compose run --rm spark python scripts/apply_live_ddl.py" -ForegroundColor Red
} elseif ($LASTEXITCODE -ne 0) { Fail "could not apply the live DDL (warehouse/live_ddl.sql)" }
docker compose up -d grafana *> $null
if ($LASTEXITCODE -ne 0) { Write-Host "WARNING: Grafana did not start (docker compose up -d grafana)" -ForegroundColor Yellow }

# seed: never reuse one (same seed = same ids = Silver drops them as duplicates)
$stateDir = Join-Path $root 'data\state'; New-Item -ItemType Directory -Force $stateDir | Out-Null
$seedFile = Join-Path $stateDir 'next_seed.txt'
if ($Seed -eq 0) { $Seed = if (Test-Path $seedFile) { [int](Get-Content $seedFile) } else { 21 } }
Set-Content $seedFile ($Seed + 1)

function Open-Window($title, $command) {
    $cmd = "`$Host.UI.RawUI.WindowTitle = '$title'; Set-Location '$root'; $command"
    Start-Process powershell -ArgumentList '-NoExit', '-Command', $cmd | Out-Null
}

Write-Host "Starting windows (seed $Seed, universe $UniverseSeed, $Events events at $Rate/s) ..."
Open-Window 'FS 1 Kafka listener' 'docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --include ''app.events|orders|delivery.events'' --property print.topic=true --property print.key=true'
Open-Window 'FS 2 Pipeline' "docker compose run --rm --name fs-pipeline -e 'FS_SPARK_MASTER=local[4]' spark python -m streaming.jobs.pipeline --lake data/lake --checkpoints data/checkpoints --show $Sample --live-store"
Write-Host "Waiting $StartDelay s for Spark to start, then the generator starts ..."
Start-Sleep $StartDelay
$foreverFlag = if ($Once) { '' } else { ' --forever' }
$inventoryArgs = if ($Inventory) { ' --inventory --scenario hero-stockout' } else { '' }
Open-Window 'FS 3 Generator' ".venv\Scripts\python scripts/run_simulator.py --live --rate $Rate --events $Events --seed $Seed --universe-seed $UniverseSeed --bootstrap localhost:9092 --show --show-every $ShowEvery$foreverFlag$inventoryArgs"
if ($SyncEvery -gt 0) {
    Open-Window 'FS 4 Gold sync' ".\scripts\sync_gold.ps1 -Every $SyncEvery"
}
Write-Host "All windows are open. Stop with .\scripts\stop_demo.ps1" -ForegroundColor Green
Write-Host "Grafana: http://localhost:3000   (live and health panels refresh every 5 s)" -ForegroundColor Green
