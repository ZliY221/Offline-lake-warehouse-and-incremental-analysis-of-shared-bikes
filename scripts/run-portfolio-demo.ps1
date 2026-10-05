[CmdletBinding()]
param(
    [string]$DemoRoot = "build/portfolio-demo",
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
$repoRoot = Split-Path -Parent $PSScriptRoot
$normalizedDemoRoot = $DemoRoot.Replace("\", "/").TrimEnd("/")
$demoLakehouse = "$normalizedDemoRoot/lakehouse"
$bronze = "$demoLakehouse/bronze/trips"
$silverValid = "$demoLakehouse/silver/trips_valid"
$silverRejected = "$demoLakehouse/silver/trips_rejected"
$silverDuplicates = "$demoLakehouse/silver/trips_duplicates"
$stationDimension = "$demoLakehouse/dim/stations"
$stationRejected = "$demoLakehouse/dim/stations_rejected"
$goldDaily = "$demoLakehouse/gold/daily_metrics"
$goldRoutes = "$demoLakehouse/gold/popular_routes"
$goldRetention = "$demoLakehouse/gold/cohort_retention"
$bronzeManifests = "$demoLakehouse/control/bronze_batches"
$backfillManifests = "$demoLakehouse/control/date_backfills"
$qualityReport = "$normalizedDemoRoot/reports/data-quality.json"
$planJson = "$normalizedDemoRoot/reports/spark-plan-analysis.json"
$planMarkdown = "$normalizedDemoRoot/reports/spark-plan-analysis.md"

Push-Location $repoRoot
try {
    & (Join-Path $PSScriptRoot "build-bronze.ps1") `
        -OutputPath $bronze `
        -ManifestPath $bronzeManifests
    & (Join-Path $PSScriptRoot "build-silver.ps1") `
        -BronzePath $bronze `
        -ValidPath $silverValid `
        -RejectedPath $silverRejected `
        -DuplicatePath $silverDuplicates
    & (Join-Path $PSScriptRoot "build-station-dimension.ps1") `
        -DimensionPath $stationDimension `
        -RejectedPath $stationRejected
    & (Join-Path $PSScriptRoot "build-gold.ps1") `
        -SilverTripPath $silverValid `
        -StationDimensionPath $stationDimension `
        -DailyMetricsPath $goldDaily `
        -PopularRoutesPath $goldRoutes `
        -CohortRetentionPath $goldRetention
    & (Join-Path $PSScriptRoot "build-quality-report.ps1") `
        -BronzePath $bronze `
        -SilverValidPath $silverValid `
        -SilverRejectedPath $silverRejected `
        -SilverDuplicatePath $silverDuplicates `
        -StationDimensionPath $stationDimension `
        -StationRejectedPath $stationRejected `
        -GoldDailyMetricsPath $goldDaily `
        -GoldPopularRoutesPath $goldRoutes `
        -GoldCohortRetentionPath $goldRetention `
        -BronzeManifestPath $bronzeManifests `
        -BackfillManifestPath $backfillManifests `
        -OutputPath $qualityReport

    $quality = Get-Content -LiteralPath $qualityReport -Raw | ConvertFrom-Json
    if ($quality.overall_status -ne "PASS") {
        throw "Portfolio demo quality gate returned $($quality.overall_status)."
    }
    & (Join-Path $PSScriptRoot "explain-spark-plans.ps1") `
        -Rows 1000 `
        -JsonPath $planJson `
        -MarkdownPath $planMarkdown
    $plan = Get-Content -LiteralPath $planJson -Raw | ConvertFrom-Json
    if (
        $plan.baseline_sort_merge.features.SortMergeJoin -lt 1 -or
        $plan.explicit_broadcast.features.BroadcastHashJoin -lt 1 -or
        $plan.adaptive_aggregation.features.AdaptiveSparkPlan -lt 1
    ) {
        throw "Portfolio demo Spark plan gate did not observe the required operators."
    }
    if (-not $SkipTests) {
        & (Join-Path $PSScriptRoot "test-all.ps1")
    }
    [ordered]@{
        status = "PASS"
        quality_checks = @($quality.checks).Count
        bronze_rows = $quality.dataset_counts.bronze_rows
        silver_valid_rows = $quality.dataset_counts.silver_valid_rows
        station_dimension_rows = $quality.dataset_counts.station_dimension_rows
        gold_daily_metric_rows = $quality.dataset_counts.gold_daily_metric_rows
        gold_popular_route_rows = $quality.dataset_counts.gold_popular_route_rows
        gold_cohort_retention_rows = $quality.dataset_counts.gold_cohort_retention_rows
        automated_tests = if ($SkipTests) { "SKIPPED" } else { "PASS" }
        report = $qualityReport
        spark_plan_analysis = "PASS"
        spark_plan_report = $planJson
    } | ConvertTo-Json
}
finally {
    Pop-Location
}
