#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../.." && pwd)"
tools_root="$repo_root/build/tools"

export JAVA_HOME="$tools_root/jdk17"
export PATH="$JAVA_HOME/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
export PYTHONPATH="$tools_root/python-packages:$repo_root/src"
export PYSPARK_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1

if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
    echo "Project-local JDK is missing. Run scripts/setup-wsl-tools.ps1 first." >&2
    exit 1
fi
if [[ ! -d "$tools_root/python-packages/pyspark" ]]; then
    echo "Isolated PySpark package is missing. Run scripts/setup-wsl-tools.ps1 first." >&2
    exit 1
fi

cd -- "$repo_root"
