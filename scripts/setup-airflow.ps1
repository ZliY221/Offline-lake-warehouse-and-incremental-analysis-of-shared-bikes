[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
$wslRepoRoot = ConvertTo-WslProjectPath -WindowsPath $repoRoot
$distro = Get-WslDistroName

& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/setup-airflow.sh"
if ($LASTEXITCODE -ne 0) { throw "Project-local Airflow setup failed in WSL." }
