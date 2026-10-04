[CmdletBinding()]
param(
    [string[]]$InputPaths = @(
        "data/sample/stations.ndjson",
        "data/sample/stations-2026-10-02.ndjson"
    ),
    [string]$DimensionPath = "build/lakehouse/dim/stations",
    [string]$RejectedPath = "build/lakehouse/dim/stations_rejected"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-WslSparkTools -RepoRoot $repoRoot)) {
    & (Join-Path $PSScriptRoot "setup-wsl-tools.ps1")
    if ($LASTEXITCODE -ne 0) { throw "WSL Spark tool setup failed." }
}
$arguments = [System.Collections.Generic.List[string]]::new()
foreach ($inputPath in $InputPaths) {
    $arguments.Add("--input")
    $arguments.Add($inputPath)
}
$arguments.Add("--dimension")
$arguments.Add($DimensionPath)
$arguments.Add("--rejected")
$arguments.Add($RejectedPath)
$wslRepoRoot = ConvertTo-WslProjectPath -WindowsPath $repoRoot
$distro = Get-WslDistroName
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-station-dimension.sh" @arguments
if ($LASTEXITCODE -ne 0) { throw "Station dimension build failed in WSL." }
