[CmdletBinding()]
param(
    [string]$SilverTripPath = "build/lakehouse/silver/trips_valid",
    [string]$StationDimensionPath = "build/lakehouse/dim/stations",
    [string]$DailyMetricsPath = "build/lakehouse/gold/daily_metrics",
    [string]$PopularRoutesPath = "build/lakehouse/gold/popular_routes",
    [int]$RouteLimit = 3
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
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-gold.sh" `
    --silver-trips $SilverTripPath `
    --station-dimension $StationDimensionPath `
    --daily-metrics $DailyMetricsPath `
    --popular-routes $PopularRoutesPath `
    --route-limit $RouteLimit
if ($LASTEXITCODE -ne 0) { throw "Gold analytics build failed in WSL." }
