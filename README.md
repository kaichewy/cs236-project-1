# CS 236 Course Project — Phase 1

Data preparation and EDA of NYC 311 and LA MyLA311 service requests with PySpark.

## Layout

```
CS236-Fall26-Project-Dataset/   raw input CSVs (800k rows each)
src/common.py                   paths, Spark session, CSV reader options
src/eda.py                      Phase 1.2 exploratory data analysis
src/process.py                  Phase 1.3 cleaning, unification, validation
run.sh                          runs a script with Java 17 + the project venv
output/eda/eda_results.md       every EDA table (generated)
output/eda/figures/*.png        EDA charts (generated)
output/processed/<dataset>/     parquet/ + <dataset>.csv for nyc_cleaned, la_cleaned, unified
output/processed/validation_results.md   cleaning log + validation checks (generated)
docs/Phase1_Report.md           report
```

## Setup (macOS)

PySpark 3.5.1 needs Java 8/11/17 and Python ≤ 3.11 is the safest choice.

```bash
brew install openjdk@17 python@3.11
```

```bash
/opt/homebrew/opt/python@3.11/bin/python3.11 -m venv .venv
```

```bash
.venv/bin/pip install pyspark==3.5.1 matplotlib pandas pyarrow
```

```bash
docker --version
```

## Run

```bash
./run.sh src/eda.py
```

```bash
./run.sh src/process.py
```

Each takes about 2–4 minutes on a laptop. `run.sh` sets `JAVA_HOME` to Java 17
and binds Spark to `127.0.0.1` (avoids a macOS `BindException`). On another
machine, set `JAVA_HOME_17` to your Java 17 home before running.
