[CmdletBinding()]
param(
    [string]$BronzePath = "build/lakehouse/bronze/trips",
    [string]$SilverValidPath = "build/lakehouse/silver/trips_valid",
    [string]$SilverRejectedPath = "build/lakehouse/silver/trips_rejected",
    [string]$SilverDuplicatePath = "build/lakehouse/silver/trips_duplicates",
    [string]$StationDimensionPath = "build/lakehouse/dim/stations",
    [string]$StationRejectedPath = "build/lakehouse/dim/stations_rejected",
    [string]$GoldDailyMetricsPath = "build/lakehouse/gold/daily_metrics",
    [string]$GoldPopularRoutesPath = "build/lakehouse/gold/popular_routes",
    [string]$GoldCohortRetentionPath = "build/lakehouse/gold/cohort_retention",
    [string]$BronzeManifestPath = "build/lakehouse/control/bronze_batches",
    [string]$BackfillManifestPath = "build/lakehouse/control/date_backfills",
    [string]$OutputPath = "build/reports/data-quality.json",
    [ValidateRange(0.0, 1.0)]
    [double]$MaxRejectedRatio = 0.05,
    [ValidateRange(0.0, 1.0)]
    [double]$MaxDuplicateRatio = 0.05,
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$RequiredLatestIngestionDate = "2026-10-01"
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
    --bronze $BronzePath `
    --silver-valid $SilverValidPath `
    --silver-rejected $SilverRejectedPath `
    --silver-duplicates $SilverDuplicatePath `
    --station-dimension $StationDimensionPath `
    --station-rejected $StationRejectedPath `
    --gold-daily-metrics $GoldDailyMetricsPath `
    --gold-popular-routes $GoldPopularRoutesPath `
    --gold-cohort-retention $GoldCohortRetentionPath `
    --bronze-manifest $BronzeManifestPath `
    --backfill-manifest $BackfillManifestPath `
    --output $OutputPath `
    --max-rejected-ratio $MaxRejectedRatio `
    --max-duplicate-ratio $MaxDuplicateRatio `
    --required-latest-ingestion-date $RequiredLatestIngestionDate
if ($LASTEXITCODE -ne 0) { throw "Quality report build failed in WSL." }
