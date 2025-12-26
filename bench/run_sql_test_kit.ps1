param(
    [ValidateSet("S1", "S2", "S4", "S6")]
    [string]$Scenario,
    [int]$N = 30,
    [int]$Warmup = 3,
    [string]$Scale = "big",
    [string]$OutDir = "data\\output"
)

$ErrorActionPreference = "Stop"
if (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

$env:DATA_SCALE = $Scale
$env:SCENARIO = $Scenario

$rawPath = Join-Path (Join-Path $RepoRoot $OutDir) "raw_runs.csv"

if (-not $Scenario) {
    throw "Scenario is required. Use -Scenario S1|S2|S4|S6."
}

function Ensure-ResultsFile {
    $dir = Split-Path $rawPath
    if (-not (Test-Path $dir)) {
        New-Item -ItemType Directory -Path $dir -Force | Out-Null
    }
    if (-not (Test-Path $rawPath)) {
        "timestamp,tool,scenario,iteration,phase,duration_ms,exit_code" | Set-Content -Path $rawPath
    }
}

function Write-RunRow {
    param(
        [string]$Tool,
        [string]$Scenario,
        [int]$Iteration,
        [string]$Phase,
        [int]$DurationMs,
        [int]$ExitCode
    )
    $timestamp = [DateTime]::UtcNow.ToString("o")
    "$timestamp,$Tool,$Scenario,$Iteration,$Phase,$DurationMs,$ExitCode" | Add-Content -Path $rawPath
}

function Ensure-PostgresContainerRunning {
    $names = docker ps -a --filter "name=postgres_tests" --format "{{.Names}}"
    if (-not $names) {
        docker run --name postgres_tests -e POSTGRES_USER=test_user -e POSTGRES_PASSWORD=test_pass -e POSTGRES_DB=test_db -p 5432:5432 -d postgres:15 | Out-Null
    } else {
        $running = docker ps --filter "name=postgres_tests" --format "{{.Names}}"
        if (-not $running) {
            docker start postgres_tests | Out-Null
        }
    }

    $deadline = (Get-Date).AddSeconds(60)
    $ready = $false
    while (-not $ready -and (Get-Date) -lt $deadline) {
        docker exec postgres_tests psql -U test_user -d postgres -c "SELECT 1;" 2>$null | Out-Null
        if ($LASTEXITCODE -eq 0) {
            $ready = $true
        } else {
            Start-Sleep -Seconds 2
        }
    }
    if (-not $ready) {
        throw "Postgres container not ready within 60 seconds."
    }

    $dbCheck = docker exec postgres_tests psql -U test_user -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='test_db';" 2>$null
    $dbCheckText = ($dbCheck | Out-String).Trim()
    $exists = $dbCheckText -eq "1"
    if ($LASTEXITCODE -ne 0 -or -not $exists) {
        docker exec postgres_tests psql -U test_user -d postgres -c "CREATE DATABASE test_db;" 2>$null | Out-Null
    }
}

function Stop-PostgresContainer {
    docker stop postgres_tests | Out-Null
    $running = docker ps --filter "name=postgres_tests" --format "{{.Names}}"
    if ($running) {
        docker stop postgres_tests | Out-Null
    }
}

function Invoke-SqlTestKitRun {
    param(
        [int]$Iteration,
        [string]$Phase
    )
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & python .\experiments\sql_test_kit_sales_aggregation\main_test.py
    $exitCode = $LASTEXITCODE
    $sw.Stop()
    Write-RunRow -Tool "sql_test_kit" -Scenario $Scenario -Iteration $Iteration -Phase $Phase -DurationMs $sw.ElapsedMilliseconds -ExitCode $exitCode
    if ($exitCode -ne 0) {
        throw "sql-test-kit failed ($Phase/$Iteration)."
    }
}

Ensure-ResultsFile

try {
    Ensure-PostgresContainerRunning

    for ($i = 1; $i -le $Warmup; $i++) {
        Invoke-SqlTestKitRun -Iteration $i -Phase "warmup"
    }

    for ($i = 1; $i -le $N; $i++) {
        Invoke-SqlTestKitRun -Iteration $i -Phase "measured"
    }
} finally {
    Stop-PostgresContainer
}
