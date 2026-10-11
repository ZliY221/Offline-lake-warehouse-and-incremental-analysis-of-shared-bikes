[CmdletBinding()]
param(
    [Parameter(Mandatory)][string]$SourceRoot,
    [string]$Output = "build/repair/source-catalog.json"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
$pythonCommand = Find-PythonCommand
$previousPythonPath = $env:PYTHONPATH
Push-Location $repoRoot
try {
    $env:PYTHONPATH = Join-Path $repoRoot "src"
    & $pythonCommand.Executable @($pythonCommand.Arguments) `
        -m bike_lakehouse.repair_cli catalog --source-root $SourceRoot --output $Output
    if ($LASTEXITCODE -ne 0) { throw "Repair source catalog creation failed." }
}
finally {
    $env:PYTHONPATH = $previousPythonPath
    Pop-Location
}
