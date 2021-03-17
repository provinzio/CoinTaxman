.DEFAULT_GOAL := lint

flake8:
	flake8 . --show-source --statistics

mypy:
	mypy . --show-column-numbers

check-isort:
	isort . --check-only --diff

lint: flake8 mypy check-isort

isort:
	isort .

black:
	black src

install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt -r requirements-dev.txt

run:
	python src/main.py

.PHONY: flake8 mypy check-isort lint isort black install run
