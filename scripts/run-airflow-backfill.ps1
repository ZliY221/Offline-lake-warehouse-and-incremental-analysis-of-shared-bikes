[CmdletBinding()]
param(
    [string]$RunRoot = "build/airflow-backfill-evidence",
    [string]$EvidenceJson = "evidence/airflow-backfill-local.json",
    [string]$EvidenceMarkdown = "evidence/airflow-backfill-local.md"
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
$airflowExecutable = Join-Path $repoRoot "build\airflow-runtime\venv\bin\airflow"
if (-not (Test-Path -LiteralPath $airflowExecutable -PathType Leaf)) {
    & (Join-Path $PSScriptRoot "setup-airflow.ps1")
    if ($LASTEXITCODE -ne 0) { throw "Airflow setup failed." }
}
$wslRepoRoot = ConvertTo-WslProjectPath -WindowsPath $repoRoot
$distro = Get-WslDistroName

& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-airflow-backfill.sh" `
    $RunRoot $EvidenceJson $EvidenceMarkdown
if ($LASTEXITCODE -ne 0) { throw "Airflow backfill evidence run failed in WSL." }
