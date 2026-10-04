[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$InputPath,
    [Parameter(Mandatory = $true)]
    [string]$TargetDate,
    [string]$BronzePath = "build/lakehouse/bronze/trips",
    [string]$BronzeManifestPath = "build/lakehouse/control/bronze_batches",
    [string]$SilverValidPath = "build/lakehouse/silver/trips_valid",
    [string]$SilverRejectedPath = "build/lakehouse/silver/trips_rejected",
    [string]$SilverDuplicatePath = "build/lakehouse/silver/trips_duplicates",
    [string]$StationDimensionPath = "build/lakehouse/dim/stations",
    [string]$GoldDailyMetricsPath = "build/lakehouse/gold/daily_metrics",
    [string]$GoldPopularRoutesPath = "build/lakehouse/gold/popular_routes",
    [string]$BackfillManifestPath = "build/lakehouse/control/date_backfills"
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
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-backfill.sh" `
    --input $InputPath `
    --target-date $TargetDate `
    --bronze $BronzePath `
    --bronze-manifest $BronzeManifestPath `
    --silver-valid $SilverValidPath `
    --silver-rejected $SilverRejectedPath `
    --silver-duplicates $SilverDuplicatePath `
    --station-dimension $StationDimensionPath `
    --gold-daily-metrics $GoldDailyMetricsPath `
    --gold-popular-routes $GoldPopularRoutesPath `
    --backfill-manifest $BackfillManifestPath
if ($LASTEXITCODE -ne 0) { throw "Date backfill failed in WSL." }
