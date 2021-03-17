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

run:
	python src/main.py

.PHONY: flake8 mypy check-isort lint isort black run
