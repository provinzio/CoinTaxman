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

# Run the project
run:
	python src/main.py

# Install requirements
install:
	python -m pip install --upgrade pip
	pip install -r requirements.txt -r requirements-dev.txt

# Setup virtual environment
venv:
	python -m venv .pyenv
	.pyenv\Scripts\activate && make install	

.PHONY: flake8 mypy check-isort lint isort black format run install venv
