[CmdletBinding()]
param(
    [string]$BronzePath = "build/lakehouse/bronze/trips",
    [string]$ValidPath = "build/lakehouse/silver/trips_valid",
    [string]$RejectedPath = "build/lakehouse/silver/trips_rejected",
    [string]$DuplicatePath = "build/lakehouse/silver/trips_duplicates"
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
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-silver.sh" `
    --bronze $BronzePath `
    --valid $ValidPath `
    --rejected $RejectedPath `
    --duplicates $DuplicatePath
if ($LASTEXITCODE -ne 0) { throw "Silver build failed in WSL." }
