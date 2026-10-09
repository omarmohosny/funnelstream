<#
Presenter helper for the failure demo (G3): kill the pipeline, watch the alert, restart, prove zero duplicates.
Start the demo first with .\scripts\watch_demo.ps1 and open Grafana (http://localhost:3000). Then, one step at a time:
  .\scripts\failure_demo.ps1 status    running containers, age of the silver_job heartbeat, newest minute with events
  .\scripts\failure_demo.ps1 kill      hard-kills the pipeline container (docker kill = a real crash, no clean shutdown)
  .\scripts\failure_demo.ps1 restart   starts the pipeline again in a new window (same command as watch_demo.ps1)
  .\scripts\failure_demo.ps1 verify    duplicate check on the Silver tables (sync container, read-only)
Nothing runs by itself. The alert numbers below are from monitoring\grafana\provisioning\alerting\alerting.yaml
(rule fs-no-events: interval 1m, for 0s, query window 2 minutes; contact point group_wait 10s, group_interval 1m).
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet('kill', 'status', 'restart', 'verify')]
    [string]$Step,
    [int]$Sample = 3         # same meaning as in watch_demo.ps1 (rows printed per batch)
)
$ErrorActionPreference = 'Continue'   # docker writes progress to stderr; Stop would make that fatal in PowerShell 5.1
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Fail($m) { Write-Host "FAILED: $m" -ForegroundColor Red; exit 1 }
function Say($m) { Write-Host $m -ForegroundColor Cyan }

function Test-Docker {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) { Fail "Docker Desktop is not running." }
}

# One value from Postgres; the SQL goes in on stdin, the credentials are read inside the container (never printed).
function Query-Pg($sql) {
    $out = $sql | docker compose exec -T postgres sh -c 'psql -U $POSTGRES_USER -d $POSTGRES_DB -At'
    if ($LASTEXITCODE -ne 0) { return $null }
    return ($out | Select-Object -First 1)
}

function Show-Status {
    Say ("Time now: {0}" -f (Get-Date -Format 'HH:mm:ss'))
    Write-Host "Containers:"
    docker ps --format '{{.Names}}   {{.Status}}'
    $age = Query-Pg "SELECT coalesce(round(extract(epoch FROM now() - last_seen_at))::text, 'none') FROM pipeline_heartbeat WHERE component = 'silver_job'"
    $newest = Query-Pg "SELECT coalesce(to_char(max(minute_start) AT TIME ZONE 'Africa/Cairo', 'HH24:MI') || ' Cairo time, ' || round(extract(epoch FROM now() - max(minute_start)))::text || ' s ago', 'none') FROM live_metrics_minute"
    if ($null -eq $age -or $null -eq $newest) { Write-Host "Postgres did not answer (is the postgres container up?)" -ForegroundColor Yellow; return }
    if ($age -eq '') { $age = 'none' }
    Write-Host "silver_job heartbeat age : $age s   (alive: under 30 s; the job reports every 15 s)"
    Write-Host "newest minute with events: $newest   (moving: under about 90 s ago)"
}

switch ($Step) {
    'status' {
        Test-Docker
        Show-Status
    }
    'kill' {
        Test-Docker
        $running = docker ps --filter name=fs-pipeline --format '{{.Names}}'
        if (-not $running) { Fail "fs-pipeline is not running. Start it with .\scripts\watch_demo.ps1 or .\scripts\failure_demo.ps1 restart." }
        $t = Get-Date
        docker kill fs-pipeline
        if ($LASTEXITCODE -ne 0) { Fail "docker kill fs-pipeline failed" }
        Say ("Pipeline killed at {0}. This was a crash, not a clean stop." -f $t.ToString('HH:mm:ss'))
        Write-Host ""
        Write-Host "Say: the Spark job just died without any warning. Kafka keeps every message, the generator keeps publishing."
        Write-Host "Look at Grafana (http://localhost:3000, refresh 5 s):"
        Write-Host "  - 'seconds since the Silver job last reported' climbs past 30 s (amber) and 120 s (red)."
        Write-Host "  - The events-per-minute line falls to zero once the last minute with data is behind the line."
        Write-Host ("Alert 'No events for 2 minutes' (rule fs-no-events) fires about 1 to 3 minutes after the kill, i.e. between {0} and {1}:" -f $t.AddMinutes(1).ToString('HH:mm'), $t.AddMinutes(3).ToString('HH:mm'))
        Write-Host "  the rule runs every 1 minute, has no extra pending time, and looks at the last 2 minutes of events."
        Write-Host "  The webhook message appears in the alert sink window (python scripts\alert_sink.py) about 10 s later."
        Write-Host "Check progress any time with: .\scripts\failure_demo.ps1 status"
    }
    'restart' {
        Test-Docker
        docker rm -f fs-pipeline *> $null
        $pg = docker ps --filter name=postgres --format '{{.Names}}'
        if (-not $pg) { Write-Host "WARNING: postgres is not running; the live tables will not update (docker compose up -d postgres grafana)." -ForegroundColor Yellow }
        # Same command as watch_demo.ps1 (tests/test_check_duplicates.py keeps the two in sync). The checkpoints in
        # data\checkpoints are reused, so Spark resumes from the last committed Kafka offsets.
        $command = "docker compose run --rm --name fs-pipeline spark python -m streaming.jobs.pipeline --lake data/lake --checkpoints data/checkpoints --show $Sample --live-store"
        $cmd = "`$Host.UI.RawUI.WindowTitle = 'FS 2 Pipeline'; Set-Location '$root'; $command"
        Start-Process powershell -ArgumentList '-NoExit', '-Command', $cmd | Out-Null
        $t = Get-Date
        Say ("Pipeline restarting in a new window at {0}. Close the old FS 2 Pipeline window when convenient." -f $t.ToString('HH:mm:ss'))
        Write-Host ""
        Write-Host "Say: same command, same checkpoints. Spark resumes from the offsets it last committed and reads what Kafka kept while it was down."
        Write-Host "Look at the new window: Spark needs about 1 minute to start, then [bronze] and [silver] lines show the backlog being processed."
        Write-Host "Look at Grafana: the heartbeat age drops back under 30 s, the events line comes back (a catch-up bump is normal)."
        Write-Host ("The alert resolves at the next one-minute check after events appear again, roughly 2 to 4 minutes after {0}; the RESOLVED line shows in the alert sink." -f $t.ToString('HH:mm'))
        Write-Host "When the line is moving again, run: .\scripts\failure_demo.ps1 verify"
    }
    'verify' {
        Test-Docker
        Say "Duplicate check on the Silver tables (read-only, runs next to the pipeline in the sync container) ..."
        docker rm -f fs-check *> $null
        docker compose run --rm --name fs-check sync python scripts/check_duplicates.py
        $code = $LASTEXITCODE
        Write-Host ""
        if ($code -eq 0) { Write-Host "Result: zero duplicate keys in Silver after the crash and restart." -ForegroundColor Green }
        elseif ($code -eq 1) { Write-Host "Result: DUPLICATES FOUND. See the lines above." -ForegroundColor Red }
        else { Write-Host "Result: the check could not run (exit $code). A Silver table is missing or the lake path is wrong." -ForegroundColor Yellow }
        Write-Host "Say: Silver is written with a MERGE that only inserts keys it has not seen, so a replayed batch after the crash adds nothing."
        Write-Host "Note: this checks Silver only. Bronze is append-only and may hold re-delivered messages; Silver removes them."
        exit $code
    }
    default {
        Write-Host "Usage: .\scripts\failure_demo.ps1 status | kill | restart | verify"
        exit 2
    }
}
