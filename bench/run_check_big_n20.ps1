param(
    [string]$OutDir = "data\\output",
    [string]$Scale = "big"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
Set-Location $RepoRoot

$outPath = Join-Path $RepoRoot $OutDir
if (-not (Test-Path $outPath)) {
    New-Item -ItemType Directory -Path $outPath -Force | Out-Null
}

$logsDir = Join-Path $RepoRoot "logs"
if (-not (Test-Path $logsDir)) {
    New-Item -ItemType Directory -Path $logsDir -Force | Out-Null
}
$logPath = Join-Path $logsDir "check_big_n20.log"

function Get-MskTimestamp {
    $tz = [TimeZoneInfo]::FindSystemTimeZoneById("Russian Standard Time")
    $now = [TimeZoneInfo]::ConvertTime([DateTimeOffset]::UtcNow, $tz)
    return $now.ToString("o")
}

function Write-Log {
    param(
        [string]$Message,
        [string]$Path
    )
    $timestamp = Get-MskTimestamp
    "$timestamp $Message" | Add-Content -Path $Path -Encoding utf8
}

function Ensure-LogFile {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        New-Item -ItemType File -Path $Path -Force | Out-Null
    }
}

function Append-FileContent {
    param(
        [string]$Source,
        [string[]]$Destinations
    )
    if (Test-Path $Source) {
        $content = Get-Content -Raw -Path $Source
        foreach ($dest in $Destinations) {
            $content | Add-Content -Path $dest -Encoding utf8
        }
        Remove-Item -Force -ErrorAction SilentlyContinue $Source
    }
}

function Get-ExistingSuccessCount {
    param(
        [string]$RawPath,
        [string]$Tool,
        [string]$Scenario
    )
    if (-not (Test-Path $RawPath)) {
        return 0
    }
    $info = Get-Item $RawPath
    if ($info.Length -eq 0) {
        return 0
    }
    $rows = Import-Csv -Path $RawPath
    ($rows | Where-Object { $_.tool -eq $Tool -and $_.scenario -eq $Scenario -and $_.phase -eq "measured" -and $_.exit_code -eq "0" }).Count
}

function Invoke-ToolRun {
    param(
        [string]$Tool,
        [string]$ScriptPath,
        [string]$Scenario,
        [int]$Remaining,
        [string]$ComboLogPath
    )
    Write-Log "START tool=$Tool scenario=$Scenario remaining=$Remaining" -Path $logPath
    Write-Log "START tool=$Tool scenario=$Scenario remaining=$Remaining" -Path $ComboLogPath
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $outFile = [IO.Path]::GetTempFileName()
    $errFile = [IO.Path]::GetTempFileName()
    try {
        $argList = @(
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            $ScriptPath,
            "-Scenario",
            $Scenario,
            "-N",
            $Remaining,
            "-Warmup",
            "0",
            "-Scale",
            $Scale,
            "-OutDir",
            $OutDir
        )
        $proc = Start-Process -FilePath "powershell" -ArgumentList $argList -NoNewWindow -Wait -PassThru -RedirectStandardOutput $outFile -RedirectStandardError $errFile
        $exitCode = $proc.ExitCode
    } finally {
        $sw.Stop()
    }

    Append-FileContent -Source $outFile -Destinations @($logPath, $ComboLogPath)
    Append-FileContent -Source $errFile -Destinations @($logPath, $ComboLogPath)

    $durationMs = $sw.ElapsedMilliseconds
    $durationSec = [Math]::Round($sw.Elapsed.TotalSeconds, 3)

    if ($exitCode -ne 0) {
        Write-Log "FAIL tool=$Tool scenario=$Scenario duration_ms=$durationMs duration_s=$durationSec exit_code=$exitCode" -Path $logPath
        Write-Log "FAIL tool=$Tool scenario=$Scenario duration_ms=$durationMs duration_s=$durationSec exit_code=$exitCode" -Path $ComboLogPath
        throw "Runner failed: $Tool $Scenario"
    }

    Write-Log "END tool=$Tool scenario=$Scenario duration_ms=$durationMs duration_s=$durationSec exit_code=$exitCode" -Path $logPath
    Write-Log "END tool=$Tool scenario=$Scenario duration_ms=$durationMs duration_s=$durationSec exit_code=$exitCode" -Path $ComboLogPath
}

$rawPath = Join-Path $outPath "raw_runs.csv"

Ensure-LogFile -Path $logPath

Write-Log "CHECK START scale=$Scale n=20 warmup=0" -Path $logPath

$scenarios = @("S1", "S2", "S4", "S6")
$tools = @(
    @{ Name = "pytest_sqlalchemy"; Script = ".\\bench\\run_pytest_sqlalchemy.ps1" },
    @{ Name = "pytest_testcontainers"; Script = ".\\bench\\run_pytest_testcontainers.ps1" },
    @{ Name = "dbt"; Script = ".\\bench\\run_dbt.ps1" },
    @{ Name = "sql_test_kit"; Script = ".\\bench\\run_sql_test_kit.ps1" }
)

try {
    foreach ($scenario in $scenarios) {
        foreach ($tool in $tools) {
            $scriptPath = Resolve-Path $tool.Script
            $comboLogPath = Join-Path $logsDir ("check_big_n20_{0}_{1}.log" -f $tool.Name, $scenario)
            Ensure-LogFile -Path $comboLogPath
            $existing = Get-ExistingSuccessCount -RawPath $rawPath -Tool $tool.Name -Scenario $scenario
            if ($existing -ge 20) {
                Write-Log "SKIP tool=$($tool.Name) scenario=$scenario existing_success=$existing" -Path $logPath
                Write-Log "SKIP tool=$($tool.Name) scenario=$scenario existing_success=$existing" -Path $comboLogPath
                continue
            }
            $remaining = 20 - $existing
            if ($existing -gt 0) {
                Write-Log "RESUME tool=$($tool.Name) scenario=$scenario existing_success=$existing remaining=$remaining" -Path $logPath
                Write-Log "RESUME tool=$($tool.Name) scenario=$scenario existing_success=$existing remaining=$remaining" -Path $comboLogPath
            }
            Invoke-ToolRun -Tool $tool.Name -ScriptPath $scriptPath -Scenario $scenario -Remaining $remaining -ComboLogPath $comboLogPath
        }
    }
    Write-Log "AGGREGATE START" -Path $logPath
    & python .\bench\aggregate_results.py *>&1 | Add-Content -Path $logPath -Encoding utf8
    if ($LASTEXITCODE -ne 0) {
        throw "aggregate_results.py failed."
    }
    Write-Log "CHECK END" -Path $logPath
} catch {
    Write-Log "ABORT error=$($_.Exception.Message)" -Path $logPath
    exit 1
}
