[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$demoRoot = "build/demo"
& (Join-Path $PSScriptRoot "build-bronze.ps1") `
    -InputPath "data/quality/trips_with_quality_issues.ndjson" `
    -OutputPath "$demoRoot/bronze/trips" `
    -IngestionDate "2026-10-01"
if ($LASTEXITCODE -ne 0) { throw "Demo Bronze build failed." }
& (Join-Path $PSScriptRoot "build-silver.ps1") `
    -BronzePath "$demoRoot/bronze/trips" `
    -ValidPath "$demoRoot/silver/trips_valid" `
    -RejectedPath "$demoRoot/silver/trips_rejected" `
    -DuplicatePath "$demoRoot/silver/trips_duplicates"
if ($LASTEXITCODE -ne 0) { throw "Demo Silver build failed." }
