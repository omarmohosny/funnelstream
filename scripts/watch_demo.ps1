<#
Opens four windows so you can watch data move through every stage, live:
  1. Kafka listener   : every message as it arrives on app.events, orders, delivery.events
  2. Bronze           : Spark ingest, a progress line per batch and the newest raw messages
  3. Silver           : Spark clean, per-batch counts and the newest cleaned rows (plus quarantine)
  4. Generator        : the simulator in live mode, printing the events it publishes
Run from anywhere:  .\scripts\watch_demo.ps1   [-Events 3000] [-Rate 50] [-Seed N] [-ShowEvery 5] [-Sample 3]
Stop:               .\scripts\stop_demo.ps1   (then close the windows)
#>
param(
    [int]$Events = 3000,
    [int]$Rate = 50,
    [int]$Seed = 0,          # 0 = next unused seed (kept in data\state\next_seed.txt), so ids never repeat
    [int]$UniverseSeed = 7,  # same customers and products on every run
    [int]$ShowEvery = 5,     # generator prints every Nth event
    [int]$Sample = 3,        # Bronze and Silver print the newest N rows per batch
    [int]$StartDelay = 75    # seconds Spark gets to start before the generator begins
)
$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Fail($m) { Write-Host "FAILED: $m" -ForegroundColor Red; exit 1 }

# Docker up?
docker info *> $null
if ($LASTEXITCODE -ne 0) { Fail "Docker Desktop is not running. Start it, wait a minute, run this again." }
if (-not (Test-Path .venv\Scripts\python.exe)) { Fail ".venv is missing (python -m venv .venv; .venv\Scripts\python -m pip install -r requirements.txt)" }

# memory: two Spark containers + Kafka need about 7 of 8 GB
docker compose stop postgres 2>&1 | Out-Null
docker rm -f fs-bronze fs-silver 2>&1 | Out-Null

# Kafka + topics
docker compose up -d kafka 2>&1 | Out-Null
$ready = $false
for ($i = 0; $i -lt 30; $i++) {
    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list *> $null
    if ($LASTEXITCODE -eq 0) { $ready = $true; break }
    Start-Sleep 3
}
if (-not $ready) { Fail "Kafka did not become ready" }
.venv\Scripts\python kafka/create_topics.py | Out-Null

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
Open-Window 'FS 1 Kafka listener' 'docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:19092 --include "app.events|orders|delivery.events" --property print.topic=true --property print.key=true'
Open-Window 'FS 2 Bronze' "docker compose run --rm --name fs-bronze spark python -m streaming.jobs.bronze_ingest --lake data/lake --checkpoints data/checkpoints --show $Sample"
Start-Sleep 20
Open-Window 'FS 3 Silver' "docker compose run --rm --name fs-silver spark python -m streaming.jobs.silver_clean --lake data/lake --checkpoints data/checkpoints --show $Sample"
Write-Host "Waiting $StartDelay s for Spark to start, then the generator starts ..."
Start-Sleep $StartDelay
Open-Window 'FS 4 Generator' ".venv\Scripts\python scripts/run_simulator.py --live --rate $Rate --events $Events --seed $Seed --universe-seed $UniverseSeed --bootstrap localhost:9092 --show --show-every $ShowEvery"
Write-Host "All four windows are open. Stop with .\scripts\stop_demo.ps1" -ForegroundColor Green
