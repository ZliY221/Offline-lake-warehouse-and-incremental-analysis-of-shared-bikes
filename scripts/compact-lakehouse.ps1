param(
    [string]$Dataset = "build/lakehouse/silver/trips_valid",
    [string]$PartitionColumn = "business_date",
    [int]$TargetSizeMb = 128,
    [int]$MinFiles = 4,
    [switch]$Execute
)

$ErrorActionPreference = "Stop"
$arguments = @(
    "-m", "bike_lakehouse.compaction_cli",
    "--dataset", $Dataset,
    "--partition-column", $PartitionColumn,
    "--target-size-mb", $TargetSizeMb,
    "--min-files", $MinFiles
)
if ($Execute) {
    $arguments += "--execute"
}
python @arguments
if ($LASTEXITCODE -ne 0) {
    throw "Compaction command failed with exit code $LASTEXITCODE"
}
