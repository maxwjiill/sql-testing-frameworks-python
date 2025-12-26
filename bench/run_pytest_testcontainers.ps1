param(
    [ValidateSet("S1", "S2", "S4", "S6")]
    [string]$Scenario,
    [int]$N = 30,
    [int]$Warmup = 3,
    [string]$Scale = "big",
    [string]$OutDir = "data\\output"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

$env:DATA_SCALE = $Scale

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

function Cleanup-Testcontainers {
    $ids = docker ps -a --filter "label=org.testcontainers" --format "{{.ID}}"
    if ($ids) {
        $ids | ForEach-Object { docker rm -f $_ | Out-Null }
    }
}

function Stop-PostgresContainer {
    docker stop postgres_tests | Out-Null
}

function Invoke-TestRun {
    param(
        [string]$Scenario,
        [string]$Filter,
        [int]$Iteration,
        [string]$Phase
    )
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & pytest -q .\experiments\pytest_testcontainers_postgres\test_container_postgres.py -k $Filter
    $exitCode = $LASTEXITCODE
    $sw.Stop()
    Write-RunRow -Tool "pytest_testcontainers" -Scenario $Scenario -Iteration $Iteration -Phase $Phase -DurationMs $sw.ElapsedMilliseconds -ExitCode $exitCode
    if ($exitCode -ne 0) {
        throw "pytest_testcontainers failed for $Scenario ($Phase/$Iteration)."
    }
}

Ensure-ResultsFile

$scenarioMap = @{
    "S1" = "test_s1_monthly_sum_equals_total"
    "S2" = "test_s2_category_sum_equals_total"
    "S4" = "test_s4_revenue_not_null_and_non_negative"
    "S6" = "test_s6_topn_per_customer"
}
$filter = $scenarioMap[$Scenario]

try {
    for ($i = 1; $i -le $Warmup; $i++) {
        Invoke-TestRun -Scenario $Scenario -Filter $filter -Iteration $i -Phase "warmup"
    }

    for ($i = 1; $i -le $N; $i++) {
        Invoke-TestRun -Scenario $Scenario -Filter $filter -Iteration $i -Phase "measured"
    }
} finally {
    Cleanup-Testcontainers
    Stop-PostgresContainer
}
