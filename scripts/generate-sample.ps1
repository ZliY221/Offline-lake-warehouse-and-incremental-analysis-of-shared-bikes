[CmdletBinding()]
param(
    [int]$Count = 20,
    [int]$Seed = 2027,
    [string]$BatchDate = "2026-10-01"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonCommand = Find-PythonCommand
$pythonExecutable = $pythonCommand.Executable
$pythonArguments = $pythonCommand.Arguments
$previousPythonPath = $env:PYTHONPATH
Push-Location $repoRoot
try {
    $env:PYTHONPATH = Join-Path $repoRoot "src"
    & $pythonExecutable @pythonArguments -m bike_lakehouse.cli `
        --date $BatchDate `
        --count $Count `
        --seed $Seed `
        --trips-output "data/sample/trips.ndjson" `
        --stations-output "data/sample/stations.ndjson"
    if ($LASTEXITCODE -ne 0) { throw "Sample generation failed." }
}
finally {
    $env:PYTHONPATH = $previousPythonPath
    Pop-Location
}
