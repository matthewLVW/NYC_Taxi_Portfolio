\
.PHONY: bronze silver gold docs demo clean

VENV=.venv
PY=$(VENV)/bin/python
PIP=$(VENV)/bin/pip
DBT=$(VENV)/bin/dbt

# Windows fallback
ifeq ($(OS),Windows_NT)
PY=$(VENV)/Scripts/python.exe
PIP=$(VENV)/Scripts/pip.exe
DBT=$(VENV)/Scripts/dbt.exe
endif

setup:
	python -m venv $(VENV) && $(PIP) install -r requirements.txt

bronze:
	$(PY) scripts/bronze_build.py --raw-dir data/raw --out data/bronze/bronze_trips.parquet

silver:
	$(PY) scripts/silver_split.py --bronze data/bronze/bronze_trips.parquet --outdir data/silver

gold:
	$(DBT) deps || true
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
