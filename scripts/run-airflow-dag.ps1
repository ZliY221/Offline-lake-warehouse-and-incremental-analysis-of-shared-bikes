[CmdletBinding()]
param(
    [string]$RunRoot = "build/airflow-dag-evidence",
    [string]$EvidenceJson = "evidence/airflow-dag-test-local.json",
    [string]$EvidenceMarkdown = "evidence/airflow-dag-test-local.md"
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

& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-airflow-dag.sh" `
    $RunRoot $EvidenceJson $EvidenceMarkdown
if ($LASTEXITCODE -ne 0) { throw "Airflow DAG test failed in WSL." }
