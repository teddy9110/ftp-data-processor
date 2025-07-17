# We are using a Makefile as a command runner for the project

# Run `make {function}` to execute the commands e.g. `make coverage` to run code tests & coverage


# Common variables
PIP_UPDATE = rm -f uv.lock.txt && uv pip compile pyproject.toml --output-file uv.lock.txt > /dev/null && uv pip sync uv.lock.txt

PIP_UPDATE_DEV = rm -f uv.lock.txt && uv pip compile pyproject.toml --extra dev --output-file uv.lock.txt > /dev/null && uv pip sync uv.lock.txt

# Targets
.PHONY: dependencies dependencies-dev coverage mypy format upgrade-db start-watchers start-prefect


# Common target for pip updating, used by multiple targets as an initial step
dependencies:
	$(PIP_UPDATE)

dependencies-dev:
	$(PIP_UPDATE_DEV)

start-watchers:
	uv run python app/pipelines/run_watchers.py

start-prefect:
	uv run prefect server start

# Run code tests & coverage
coverage:
	uv run pytest --cov=app --cov-report=term-missing --cov-report=term -s -vv


# Run mypy static type checker
mypy:
	uv run mypy app --ignore-missing-imports


# Format all files in the app/ & tests/ directories
format:
	uv run ruff check --select I --fix app/ tests/
	uv run ruff format app/ tests/


# Run alembic migrations
upgrade-db:
	uv run alembic upgrade head
