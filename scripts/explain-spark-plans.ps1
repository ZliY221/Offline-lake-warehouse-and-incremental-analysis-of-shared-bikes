[CmdletBinding()]
param(
    [int]$Rows = 1000,
    [string]$JsonPath = "build/reports/spark-plan-analysis.json",
    [string]$MarkdownPath = "build/reports/spark-plan-analysis.md"
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
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-plan-analysis.sh" `
    --rows $Rows `
    --json $JsonPath `
    --markdown $MarkdownPath
if ($LASTEXITCODE -ne 0) { throw "Spark plan analysis failed in WSL." }
