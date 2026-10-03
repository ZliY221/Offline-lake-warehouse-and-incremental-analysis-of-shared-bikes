function Find-Jdk17Home {
    $candidateHomes = [System.Collections.Generic.List[string]]::new()
    if ($env:JAVA_HOME) { $candidateHomes.Add($env:JAVA_HOME) }
    if ($env:OS -eq "Windows_NT") {
        foreach ($searchRoot in @(
            (Join-Path $env:ProgramFiles "Microsoft"),
            (Join-Path $env:ProgramFiles "Eclipse Adoptium"),
            (Join-Path $env:ProgramFiles "Java")
        )) {
            if (Test-Path -LiteralPath $searchRoot -PathType Container) {
                Get-ChildItem -LiteralPath $searchRoot -Directory -ErrorAction SilentlyContinue |
                    Where-Object Name -Like "jdk-17*" |
                    Sort-Object Name -Descending |
                    ForEach-Object { $candidateHomes.Add($_.FullName) }
            }
        }
    }
    foreach ($candidateHome in $candidateHomes) {
        $javaPath = Join-Path $candidateHome "bin\java.exe"
        if (Test-Path -LiteralPath $javaPath -PathType Leaf) {
            $versionText = (& $javaPath -version 2>&1 | Out-String)
            if ($versionText -match 'version "(?<major>\d+)' -and [int]$Matches.major -ge 17) {
                return $candidateHome
            }
        }
    }
    throw "JDK 17 or newer was not found. Install JDK 17 or set JAVA_HOME."
}

function Find-PythonCommand {
    if ($env:PYTHON) {
        if (-not (Get-Command $env:PYTHON -ErrorAction SilentlyContinue)) {
            throw "The Python executable configured in PYTHON was not found."
        }
        return [PSCustomObject]@{ Executable = $env:PYTHON; Arguments = @() }
    }
    foreach ($candidate in @("python", "python3", "py")) {
        if (Get-Command $candidate -ErrorAction SilentlyContinue) {
            if ($candidate -eq "py") {
                return [PSCustomObject]@{ Executable = "py"; Arguments = @("-3") }
            }
            return [PSCustomObject]@{ Executable = $candidate; Arguments = @() }
        }
    }
    throw "Python 3 was not found."
}

function Get-WslDistroName {
    if ($env:BIKE_LAKEHOUSE_WSL_DISTRO) {
        return $env:BIKE_LAKEHOUSE_WSL_DISTRO
    }
    return "Ubuntu"
}

function ConvertTo-WslProjectPath {
    param([Parameter(Mandatory)][string]$WindowsPath)

    $resolved = (Resolve-Path -LiteralPath $WindowsPath).Path
    if ($resolved -notmatch '^(?<drive>[A-Za-z]):(?<tail>\\.*)$') {
        throw "Only a local Windows drive path can be converted to WSL: $resolved"
    }
    $drive = $Matches.drive.ToLowerInvariant()
    $tail = $Matches.tail.Replace("\", "/")
    return "/mnt/$drive$tail"
}

function Test-WslSparkTools {
    param([Parameter(Mandatory)][string]$RepoRoot)

    return (
        (Test-Path -LiteralPath (Join-Path $RepoRoot "build\tools\jdk17\bin\java") -PathType Leaf) -and
        (Test-Path -LiteralPath (Join-Path $RepoRoot "build\tools\python-packages\pyspark") -PathType Container) -and
        (Test-Path -LiteralPath (Join-Path $RepoRoot "build\tools\python-packages\py4j") -PathType Container)
    )
}
