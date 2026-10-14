#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../.." && pwd)"
cache_root="${XDG_CACHE_HOME:-$HOME/.cache}"
managed_root="$cache_root/zliy221"
tools_root="$managed_root/bike-trip-lakehouse"
source_dir="$repo_root/build/tools/python-packages"
target="$tools_root/python-packages"
staging="$tools_root/python-packages-staging"

if [[ ! -d "$source_dir/pyspark" || ! -d "$source_dir/py4j" ]]; then
    echo "Staged PySpark packages are missing: $source_dir" >&2
    exit 1
fi
case "$staging" in
    "$managed_root"/*) ;;
    *) echo "Refusing to clean an unexpected staging path." >&2; exit 1 ;;
esac
rm -rf -- "$staging"
mkdir -p -- "$staging"
cp -a -- "$source_dir/pyspark" "$source_dir/py4j" "$staging/"
rm -rf -- "$target"
mv -- "$staging" "$target"
