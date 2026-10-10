#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../.." && pwd)"
runtime_root="$repo_root/build/airflow-runtime"
uv_root="$repo_root/build/tools/uv"
uv_bin="$uv_root/uv"
venv_root="$runtime_root/venv"
airflow_version="3.1.6"
constraints_path="$runtime_root/constraints-3.12.txt"

mkdir -p -- "$uv_root" "$runtime_root"
if [[ ! -x "$uv_bin" ]]; then
    echo "Installing uv into the project-local ignored tool directory..."
    curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR="$uv_root" sh
fi

"$uv_bin" python install 3.12
if [[ ! -x "$venv_root/bin/python" ]]; then
    "$uv_bin" venv --python 3.12 "$venv_root"
fi
if [[ ! -s "$constraints_path" ]]; then
    if ! curl -L --fail --retry 5 --retry-all-errors \
        --output "$constraints_path" \
        "https://raw.githubusercontent.com/apache/airflow/constraints-$airflow_version/constraints-3.12.txt"; then
        response_path="$runtime_root/constraints-response.json"
        curl -L --fail --retry 5 --retry-all-errors \
            --header "Accept: application/vnd.github+json" \
            --output "$response_path" \
            "https://api.github.com/repos/apache/airflow/contents/constraints-3.12.txt?ref=constraints-$airflow_version"
        python3 - "$response_path" "$constraints_path" <<'PY'
import base64
import json
from pathlib import Path
import sys

response = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
Path(sys.argv[2]).write_bytes(base64.b64decode(response["content"]))
PY
        rm -f -- "$response_path"
    fi
fi
"$uv_bin" pip install \
    --python "$venv_root/bin/python" \
    "apache-airflow==$airflow_version" \
    --constraint "$constraints_path"

"$venv_root/bin/airflow" version
