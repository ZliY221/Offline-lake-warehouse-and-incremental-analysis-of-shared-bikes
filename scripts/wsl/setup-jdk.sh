#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd -- "$script_dir/../.." && pwd)"
tools_root="$repo_root/build/tools"
archive="$tools_root/microsoft-jdk17-linux-x64.tar.gz"
target="$tools_root/jdk17"
staging="$tools_root/jdk17-staging"

if [[ -x "$target/bin/java" ]]; then
    "$target/bin/java" -version
    exit 0
fi
if [[ ! -f "$archive" ]]; then
    echo "JDK archive is missing: $archive" >&2
    exit 1
fi
case "$staging" in
    "$tools_root"/*) ;;
    *) echo "Refusing to clean an unexpected staging path." >&2; exit 1 ;;
esac
rm -rf -- "$staging"
mkdir -p -- "$staging"
tar -xzf "$archive" --strip-components=1 -C "$staging"
rm -rf -- "$target"
mv -- "$staging" "$target"
"$target/bin/java" -version
