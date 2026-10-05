[CmdletBinding()]
param(
    [int]$Rows = 50000,
    [int]$Partitions = 8,
    [string]$JsonPath = "build/reports/spark-stage-metrics.json",
    [string]$MarkdownPath = "build/reports/spark-stage-metrics.md"
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
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-stage-metrics.sh" `
    --rows $Rows `
    --partitions $Partitions `
    --json $JsonPath `
    --markdown $MarkdownPath
if ($LASTEXITCODE -ne 0) { throw "Spark stage metrics collection failed in WSL." }
