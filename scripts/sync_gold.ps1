<#
Keeps Gold fresh while the live demo runs: every -Every seconds it publishes Silver to the Databricks
mirror and rebuilds the Gold window. Runs until Ctrl+C. Uses the small `sync` container (1.5 GB) so it fits
next to the pipeline container (Docker has about 8 GB).
Run from anywhere:  .\scripts\sync_gold.ps1   [-Every 300] [-Days 7]
#>
param(
    [int]$Every = 300,   # seconds between rounds
    [int]$Days = 7       # Gold rebuild window
)
$ErrorActionPreference = 'Continue'
Set-Location (Split-Path -Parent $PSScriptRoot)

function Step($name, $cmd) {
    Write-Host ("[{0}] {1} ..." -f (Get-Date -Format 'HH:mm:ss'), $name) -ForegroundColor Cyan
    & $cmd
    if ($LASTEXITCODE -ne 0) { Write-Host ("[{0}] {1} FAILED (exit {2}); next round retries" -f (Get-Date -Format 'HH:mm:ss'), $name, $LASTEXITCODE) -ForegroundColor Red; return $false }
    Write-Host ("[{0}] {1} done" -f (Get-Date -Format 'HH:mm:ss'), $name) -ForegroundColor Green
    return $true
}

Write-Host "Gold sync every $Every s (publish Silver, Gold build, inventory daily, D-1 marketing). Ctrl+C stops it." -ForegroundColor Green
try {
    while ($true) {
        docker rm -f fs-sync fs-sync-inv *> $null
        if (Step 'publish Silver -> Databricks' { docker compose run --rm --name fs-sync sync python scripts/publish_silver.py }) {
            Step "Gold build ($Days days)" { docker compose run --rm --name fs-sync sync python -m warehouse.gold_build --days $Days } | Out-Null
        }
        Step "Inventory daily (Postgres)" { docker compose run --rm --name fs-sync-inv spark python scripts/build_inventory_daily.py --days 2 } | Out-Null
        Step "D-1 marketing (Postgres)"  { docker compose run --rm --name fs-sync sync python scripts/build_d1_marketing.py } | Out-Null
        Write-Host ("[{0}] next round in {1} s" -f (Get-Date -Format 'HH:mm:ss'), $Every)
        Start-Sleep $Every
    }
} finally {
    docker rm -f fs-sync fs-sync-inv *> $null
}
