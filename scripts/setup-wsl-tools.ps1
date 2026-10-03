[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "common-spark.ps1")
$repoRoot = Split-Path -Parent $PSScriptRoot
$toolsRoot = Join-Path $repoRoot "build\tools"
$packageRoot = Join-Path $toolsRoot "python-packages"
$archivePath = Join-Path $toolsRoot "microsoft-jdk17-linux-x64.tar.gz"
$pythonCommand = Find-PythonCommand
$pythonExecutable = $pythonCommand.Executable
$pythonArguments = $pythonCommand.Arguments

New-Item -ItemType Directory -Force -Path $toolsRoot | Out-Null
$locationsJson = & $pythonExecutable @pythonArguments -c @'
import json
from pathlib import Path
import py4j
import pyspark
print(json.dumps({"pyspark": str(Path(pyspark.__file__).parent), "py4j": str(Path(py4j.__file__).parent)}))
'@
if ($LASTEXITCODE -ne 0) {
    throw "PySpark is not installed in Windows Python. Run: python -m pip install -e ."
}
$locations = $locationsJson | ConvertFrom-Json
if (Test-Path -LiteralPath $packageRoot) {
    $resolvedTools = (Resolve-Path -LiteralPath $toolsRoot).Path
    $resolvedPackages = (Resolve-Path -LiteralPath $packageRoot).Path
    if (-not $resolvedPackages.StartsWith($resolvedTools + [System.IO.Path]::DirectorySeparatorChar)) {
        throw "Refusing to replace a package directory outside build/tools."
    }
    Remove-Item -LiteralPath $resolvedPackages -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $packageRoot | Out-Null
Copy-Item -LiteralPath $locations.pyspark -Destination $packageRoot -Recurse
Copy-Item -LiteralPath $locations.py4j -Destination $packageRoot -Recurse

if (-not (Test-Path -LiteralPath $archivePath -PathType Leaf)) {
    Write-Host "Downloading official Microsoft OpenJDK 17 for the project-local WSL runtime..."
    & curl.exe -L --fail --retry 3 -o $archivePath `
        "https://aka.ms/download-jdk/microsoft-jdk-17-linux-x64.tar.gz"
    if ($LASTEXITCODE -ne 0) { throw "Microsoft OpenJDK download failed." }
}

$wslRepoRoot = ConvertTo-WslProjectPath -WindowsPath $repoRoot
$distro = Get-WslDistroName
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/setup-jdk.sh"
if ($LASTEXITCODE -ne 0) { throw "Project-local JDK extraction failed in WSL." }
Write-Host "WSL Spark tools are ready under build/tools (Git ignored)."
