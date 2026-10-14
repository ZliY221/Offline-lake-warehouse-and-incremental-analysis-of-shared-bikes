#!/usr/bin/env bash
set -euo pipefail

cache_root="${XDG_CACHE_HOME:-$HOME/.cache}"
tools_root="$cache_root/zliy221/bike-trip-lakehouse"
test -x "$tools_root/jdk17/bin/java"
test -d "$tools_root/python-packages/pyspark"
test -d "$tools_root/python-packages/py4j"
