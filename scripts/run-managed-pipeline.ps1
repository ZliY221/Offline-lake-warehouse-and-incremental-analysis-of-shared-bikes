[CmdletBinding()]
param(
    [string]$RunRoot = "build/managed-pipeline",
    [string]$RunId = "managed-pipeline-20261005",
    [switch]$Resume,
    [string]$EvidenceJson,
    [string]$EvidenceMarkdown,
    [ValidateSet("bronze", "silver", "station_dimension", "gold", "quality_gate")]
    [string]$InjectFailureOnce
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-WslSparkTools -RepoRoot $repoRoot)) {
    & (Join-Path $PSScriptRoot "setup-wsl-tools.ps1")
    if ($LASTEXITCODE -ne 0) { throw "WSL Spark tool setup failed." }
}
$arguments = [System.Collections.Generic.List[string]]::new()
$arguments.Add("--run-root")
$arguments.Add($RunRoot)
$arguments.Add("--run-id")
$arguments.Add($RunId)
if ($Resume) { $arguments.Add("--resume") }
if ([bool]$EvidenceJson -ne [bool]$EvidenceMarkdown) {
    throw "EvidenceJson and EvidenceMarkdown must be provided together."
}
if ($EvidenceJson) {
    $arguments.Add("--evidence-json")
    $arguments.Add($EvidenceJson)
    $arguments.Add("--evidence-markdown")
    $arguments.Add($EvidenceMarkdown)
}
if ($InjectFailureOnce) {
    $arguments.Add("--inject-failure-once")
    $arguments.Add($InjectFailureOnce)
}
$wslRepoRoot = ConvertTo-WslProjectPath -WindowsPath $repoRoot
$distro = Get-WslDistroName
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-managed-pipeline.sh" @arguments
if ($LASTEXITCODE -ne 0) { throw "Managed lakehouse pipeline failed in WSL." }
