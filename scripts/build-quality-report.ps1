[CmdletBinding()]
param(
    [string]$OutputPath = "build/reports/data-quality.json"
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
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-quality-report.sh" `
    --bronze build/lakehouse/bronze/trips `
    --silver-valid build/lakehouse/silver/trips_valid `
    --silver-rejected build/lakehouse/silver/trips_rejected `
    --silver-duplicates build/lakehouse/silver/trips_duplicates `
    --station-dimension build/lakehouse/dim/stations `
    --station-rejected build/lakehouse/dim/stations_rejected `
    --gold-daily-metrics build/lakehouse/gold/daily_metrics `
    --gold-popular-routes build/lakehouse/gold/popular_routes `
    --gold-cohort-retention build/lakehouse/gold/cohort_retention `
    --bronze-manifest build/lakehouse/control/bronze_batches `
    --backfill-manifest build/lakehouse/control/date_backfills `
    --output $OutputPath
if ($LASTEXITCODE -ne 0) { throw "Quality report build failed in WSL." }
