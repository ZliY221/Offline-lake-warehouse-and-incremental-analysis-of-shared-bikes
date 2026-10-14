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
if (Test-Path -LiteralPath $packageRoot) {
    $resolvedTools = (Resolve-Path -LiteralPath $toolsRoot).Path
    $resolvedPackages = (Resolve-Path -LiteralPath $packageRoot).Path
    if (-not $resolvedPackages.StartsWith($resolvedTools + [System.IO.Path]::DirectorySeparatorChar)) {
        throw "Refusing to replace a package directory outside build/tools."
    }
    Remove-Item -LiteralPath $resolvedPackages -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $packageRoot | Out-Null
& $pythonExecutable @pythonArguments -m pip install `
    --disable-pip-version-check `
    --target $packageRoot `
    "pyspark==4.2.0"
if ($LASTEXITCODE -ne 0) { throw "Pinned PySpark staging failed." }

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
& wsl.exe -d $distro -- bash "$wslRepoRoot/scripts/wsl/setup-python-packages.sh"
if ($LASTEXITCODE -ne 0) { throw "Managed PySpark package installation failed in WSL." }
Write-Host "WSL Spark tools are ready under the WSL user cache."
