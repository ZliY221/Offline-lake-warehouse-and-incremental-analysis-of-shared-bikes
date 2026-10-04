#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$script_dir/common.sh"
if [[ $# -gt 0 ]]; then
    python3 -W error -m unittest "$@"
else
    python3 -W error -m unittest discover -s tests -v
fi
