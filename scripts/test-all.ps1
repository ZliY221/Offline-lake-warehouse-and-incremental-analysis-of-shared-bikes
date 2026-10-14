[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-WslSparkTools -RepoRoot $repoRoot)) {
    & (Join-Path $PSScriptRoot "setup-wsl-tools.ps1")
    if ($LASTEXITCODE -ne 0) { throw "WSL Spark tool setup failed." }
}
$wslRepoRoot = ConvertTo-WslProjectPath -WindowsPath $repoRoot
$distro = Get-WslDistroName
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/run-tests.sh"
if ($LASTEXITCODE -ne 0) { throw "Python or Spark tests failed in WSL." }

. (Join-Path $PSScriptRoot "python-runtime.ps1")
$pythonRuntime = Resolve-ProjectPython -MinimumVersion "3.10" -RepoRoot $repoRoot
$pythonExecutable = $pythonRuntime.Executable
$pythonArguments = @($pythonRuntime.Arguments)
& $pythonExecutable @pythonArguments (Join-Path $PSScriptRoot "audit-publication.py")
if ($LASTEXITCODE -ne 0) { throw "Publication audit failed." }
