[CmdletBinding()]
param(
    [ValidateSet("full", "backend", "frontend", "focused", "migration")]
    [string]$Profile = "full",
    [string[]]$Gate = @(),
    [string]$Label = "current-baseline",
    [string]$ResultsDir,
    [ValidateRange(1, 86400)]
    [int]$Timeout = 1200,
    [switch]$FailFast,
    [switch]$AllowBlocked,
    [switch]$SkipE2E,
    [switch]$List
)

$ErrorActionPreference = "Stop"
$RepositoryRoot = Split-Path -Parent $PSScriptRoot
$Runner = Join-Path $PSScriptRoot "run_acceptance.py"
$IsolationRoot = Join-Path ([System.IO.Path]::GetTempPath()) (
    "ai-orchestrator-acceptance-" + [guid]::NewGuid().ToString("N")
)
$IsolatedEnvironment = @{
    "ORCHESTRATOR_DB_PATH" = Join-Path $IsolationRoot "orchestrator.sqlite3"
    "ORCH_ACCOUNT_LEASE_DB" = Join-Path $IsolationRoot "account-leases.sqlite3"
    "ORCH_TASKS_FILE" = Join-Path $IsolationRoot "tasks.json"
    "ORCH_ARTIFACTS_DIR" = Join-Path $IsolationRoot "artifacts"
    "ORCH_AGENT_LOG_DIR" = Join-Path $IsolationRoot "logs\agents"
    "ORCH_SNAPSHOTS_DIR" = Join-Path $IsolationRoot "snapshots"
    "ORCH_COOKIES_DIR" = Join-Path $IsolationRoot "cookies"
    "ORCHESTRATOR_PROJECT_LOCK_ROOT" = Join-Path $IsolationRoot "project-locks"
    "ORCH_ACCOUNT_FINGERPRINT_SALT_FILE" = Join-Path $IsolationRoot (
        "secrets\account-fingerprint.salt"
    )
}
$PreviousEnvironment = @{}

$RunnerArguments = @(
    $Runner,
    "--profile", $Profile,
    "--label", $Label,
    "--timeout", $Timeout
)
foreach ($GateId in $Gate) {
    $RunnerArguments += @("--gate", $GateId)
}
if ($ResultsDir) {
    $RunnerArguments += @("--results-dir", $ResultsDir)
}
if ($FailFast) {
    $RunnerArguments += "--fail-fast"
}
if ($AllowBlocked) {
    $RunnerArguments += "--allow-blocked"
}
if ($SkipE2E) {
    $RunnerArguments += "--skip-e2e"
}
if ($List) {
    $RunnerArguments += "--list"
}

$ExitCode = 1
New-Item -ItemType Directory -Path $IsolationRoot | Out-Null
foreach ($Name in $IsolatedEnvironment.Keys) {
    $PreviousEnvironment[$Name] = [Environment]::GetEnvironmentVariable(
        $Name,
        [EnvironmentVariableTarget]::Process
    )
    $Value = $IsolatedEnvironment[$Name]
    $Parent = Split-Path -Parent $Value
    if ($Parent) {
        New-Item -ItemType Directory -Path $Parent -Force | Out-Null
    }
    [Environment]::SetEnvironmentVariable(
        $Name,
        $Value,
        [EnvironmentVariableTarget]::Process
    )
}

Push-Location $RepositoryRoot
try {
    & python @RunnerArguments
    $ExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
    foreach ($Name in $PreviousEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable(
            $Name,
            $PreviousEnvironment[$Name],
            [EnvironmentVariableTarget]::Process
        )
    }
    Remove-Item -Path $IsolationRoot -Recurse -Force -ErrorAction SilentlyContinue
}
exit $ExitCode
