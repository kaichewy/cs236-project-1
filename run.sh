#!/usr/bin/env bash
# Runs a pipeline script with the environment PySpark 3.5.1 needs:
#   - Java 17 (Spark 3.5 does not support Java 21+)
#   - the project virtualenv (Python 3.11 + pyspark==3.5.1)
# Usage: ./run.sh src/eda.py   |   ./run.sh src/process.py
set -euo pipefail
cd "$(dirname "$0")"
export JAVA_HOME="${JAVA_HOME_17:-/opt/homebrew/opt/openjdk@17}"
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON="$PWD/.venv/bin/python"
exec "$PWD/.venv/bin/python" "$@"
