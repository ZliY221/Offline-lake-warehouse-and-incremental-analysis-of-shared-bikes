#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../.." && pwd)"
source "$script_dir/common.sh"

run_root_argument="${1:-build/airflow-backfill-evidence}"
evidence_json_argument="${2:-evidence/airflow-backfill-local.json}"
evidence_markdown_argument="${3:-evidence/airflow-backfill-local.md}"
runtime_root="$repo_root/build/airflow-runtime"
airflow_home="$runtime_root/backfill-home"
airflow_python="$runtime_root/venv/bin/python"
airflow_cli="$runtime_root/venv/bin/airflow"
run_root="$(realpath -m -- "$repo_root/$run_root_argument")"
evidence_json="$(realpath -m -- "$repo_root/$evidence_json_argument")"
evidence_markdown="$(realpath -m -- "$repo_root/$evidence_markdown_argument")"

case "$run_root" in
    "$repo_root"/build/*) ;;
    *) echo "Run root must stay under the repository build directory." >&2; exit 1 ;;
esac
case "$airflow_home" in
    "$repo_root"/build/*) ;;
    *) echo "Airflow home must stay under the repository build directory." >&2; exit 1 ;;
esac
if [[ ! -x "$airflow_cli" ]]; then
    echo "Project-local Airflow is missing. Run scripts/setup-airflow.ps1 first." >&2
    exit 1
fi

rm -rf -- "$run_root" "$airflow_home"
mkdir -p -- "$run_root" "$airflow_home"

export AIRFLOW_HOME="$airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$repo_root/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__LOGGING__COLORED_CONSOLE_LOG=False
export BIKE_LAKEHOUSE_REPO_ROOT="$repo_root"
export BIKE_LAKEHOUSE_AIRFLOW_RUN_ROOT="$run_root"
export BIKE_LAKEHOUSE_AIRFLOW_FAIL_ONCE=backfill

"$airflow_cli" db migrate
"$airflow_python" "$repo_root/scripts/airflow_backfill_evidence.py" \
    --dag-folder "$repo_root/dags" \
    --run-root "$run_root" \
    --evidence-json "$evidence_json" \
    --evidence-markdown "$evidence_markdown"
