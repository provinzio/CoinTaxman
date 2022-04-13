.DEFAULT_GOAL := lint

flake8:
	flake8 . --show-source --statistics

mypy:
	mypy . --show-column-numbers

check-isort:
	isort . --check-only --diff

# Run linter
lint: flake8 mypy check-isort

isort:
	isort .

black:
	black src

# Run formatter
format: black isort

# Build dockerfile
build:
	docker build -t cointaxman:latest .

# Run the project
run:
	python src/main.py

# Shortcut for development (developer run)
drun: format lint run

run-container:
	docker run --name cointaxman -it --rm \
		-v `pwd`/account_statements:/CoinTaxman/account_statements:Z \
		-v `pwd`/data:/CoinTaxman/data:Z \
		-v `pwd`/export:/CoinTaxman/export:Z \
		-e TAX_YEAR=2021 -e COUNTRY=GERMANY \
		cointaxman

clean:
	del /S data\*.db

cleanrun: clean run

check-db:
	cd src && python -c "from price_data import PriceData; PriceData().check_database()"

# Install requirements
install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

install-dev: install
	pip -r requirements-dev.txt

# Setup virtual environment
venv:
	python3 -m venv .pyenv
ifdef OS # Windows
	.pyenv\Scripts\activate && make install
else # Linux
	source .pyenv/bin/activate && make install
endif

# Rebuild requirements(-dev).txt
build-deps:
	pip-compile --output-file requirements.txt requirements.in
	pip-compile --output-file requirements-dev.txt requirements-dev.in

archive:
	python src/archive_evaluation.py

.PHONY: flake8 mypy check-isort lint isort black format build run run-container clean cleanrun check-db install install-dev venv build-deps archive
