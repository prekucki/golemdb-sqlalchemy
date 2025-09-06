# Makefile for GolemBase SQLAlchemy Dialect project

.PHONY: help install install-dev clean test test-dialect test-app lint format setup-dev run-app

help:
	@echo "Available commands:"
	@echo "  install      - Install packages with poetry"
	@echo "  install-dev  - Install packages with dev dependencies"
	@echo "  install-app  - Install testapp dependencies"
	@echo "  setup-dev    - Full development environment setup"
	@echo "  clean        - Clean build artifacts and cache"
	@echo "  test         - Run all tests"
	@echo "  test-dialect - Run dialect tests only"
	@echo "  test-app     - Run test application tests only"
	@echo "  lint         - Run linting tools"
	@echo "  format       - Format code with ruff"
	@echo "  run-app      - Start the test application"

install:
	poetry install --only=main

install-dev:
	poetry install --with=dev

install-app:
	cd testapp && poetry install

setup-dev: install-dev install-app
	poetry run pre-commit install
	@echo "Development environment setup complete!"
	@echo "To run the test app: make run-app"
	@echo "To run tests: make test"

clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/ dist/ htmlcov/ .coverage .pytest_cache/
	cd sqlalchemy_golembase && rm -rf build/ dist/ *.egg-info/
	cd testapp && rm -rf build/ dist/ *.egg-info/

test:
	poetry run pytest sqlalchemy_golembase/tests/ -v
	cd testapp && poetry run pytest tests/ -v

test-dialect:
	poetry run pytest sqlalchemy_golembase/tests/ -v

test-app:
	cd testapp && poetry run pytest tests/ -v

lint:
	poetry run ruff check sqlalchemy_golembase/src/ testapp/src/
	poetry run mypy sqlalchemy_golembase/src/ testapp/src/

format:
	poetry run ruff format sqlalchemy_golembase/src/ testapp/src/
	poetry run ruff check --fix sqlalchemy_golembase/src/ testapp/src/

run-app:
	cd testapp && poetry run python -m testapp.main

check: lint test
	@echo "All checks passed!"

# Poetry helpers
poetry-update:
	poetry update

poetry-lock:
	poetry lock

poetry-show:
	poetry show --tree