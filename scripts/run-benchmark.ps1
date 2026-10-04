[CmdletBinding()]
param(
    [int]$Rows = 10000,
    [int]$WarmupRounds = 1,
    [int]$MeasuredRounds = 3,
    [string]$ReportPath = "build/reports/benchmark-10000-local.json"
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
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-benchmark.sh" `
    --repo-root . `
    --work build/benchmark `
    --report $ReportPath `
    --rows $Rows `
    --warmup-rounds $WarmupRounds `
    --measured-rounds $MeasuredRounds
if ($LASTEXITCODE -ne 0) { throw "Benchmark failed in WSL." }
