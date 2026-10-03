[CmdletBinding()]
param(
    [string]$InputPath = "data/sample/trips.ndjson",
    [string]$OutputPath = "build/lakehouse/bronze/trips",
    [string]$IngestionDate = "2026-10-01"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-WslSparkTools -RepoRoot $repoRoot)) {
    & (Join-Path $PSScriptRoot "setup-wsl-tools.ps1")
    if ($LASTEXITCODE -ne 0) { throw "WSL Spark tool setup failed." }
}
$wslRepoRoot = ConvertTo-WslProjectPath -WindowsPath $repoRoot
$distro = Get-WslDistroName
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-bronze.sh" `
    --input $InputPath `
    --output $OutputPath `
    --ingestion-date $IngestionDate
if ($LASTEXITCODE -ne 0) { throw "Bronze ingestion failed in WSL." }
