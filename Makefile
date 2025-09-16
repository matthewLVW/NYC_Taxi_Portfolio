\
.PHONY: data bronze silver gold docs demo clean

VENV=.venv
PY=$(VENV)/bin/python
PIP=$(VENV)/bin/pip
DBT=$(VENV)/bin/dbt

# Windows fallback
ifeq ($(OS),Windows_NT)
PY=$(VENV)\\Scripts\\python.exe
PIP=$(VENV)\\Scripts\\pip.exe
DBT=$(VENV)\\Scripts\\dbt.exe
endif

###########################
# Configurable data range #
###########################
# Defaults keep downloads small for a quick demo; override as needed:
#   make data START=2024-01 END=2024-06 SERVICE=yellow
SERVICE ?= yellow
START ?= 2024-01
END ?= $(START)
RAW_DIR ?= data/raw

setup:
	python -m venv $(VENV) && $(PIP) install -r requirements.txt

data:
	$(PY) scripts/read_raw_by_month.py --service $(SERVICE) --start $(START) --end $(END) --raw-dir $(RAW_DIR)

bronze:
	$(PY) scripts/bronze_build.py --raw-dir data/raw --out data/bronze/bronze_trips.parquet

silver:
	$(PY) scripts/silver_split.py --bronze data/bronze/bronze_trips.parquet --outdir data/silver

gold:
	-$(DBT) deps
	$(DBT) seed
	$(DBT) run
	$(DBT) test
	$(DBT) docs generate

docs:
	$(DBT) docs generate

demo: bronze silver gold

clean:
	rm -f db/warehouse.duckdb
	rm -rf target dbt/target
