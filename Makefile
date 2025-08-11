VENV_PATH ?= .venv
SOURCE_PATH ?= lubrikit
TEST_PATH ?= tests

.PHONY: help install lint-python lint-spellcheck lint-yaml type-checking format test coverage docs

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Create uv environment and install dependencies
	uv sync --extra dev

lint: lint-python lint-spellcheck lint-yaml type-checking ## Run all linting checks

lint-python: ## Run ruff linting and formatting checks (passive)
	uv run ruff check $(SOURCE_PATH) $(TEST_PATH)
	uv run ruff check --select I $(SOURCE_PATH) $(TEST_PATH)
	uv run ruff format --check $(SOURCE_PATH) $(TEST_PATH)

lint-spellcheck: ## Run typos spellchecker
	uv run typos .

lint-yaml: ## Run YAML linting
	uv run yamllint -d "{extends: relaxed, rules: {line-length: {max: 120}}}" .github

type-checking: ## Run mypy type checking
	uv run mypy --install-types --non-interactive $(SOURCE_PATH) $(TEST_PATH)

format: ## Run ruff linting and formatting (active fixes)
	uv run ruff check --fix $(SOURCE_PATH) $(TEST_PATH)
	uv run ruff check --select I --fix $(SOURCE_PATH) $(TEST_PATH)
	uv run ruff format $(SOURCE_PATH) $(TEST_PATH)

test: ## Run all unit tests
	uv run pytest $(TEST_PATH)

coverage: ## Run tests with coverage report
	uv run pytest --cov=$(SOURCE_PATH) --cov-report=term-missing $(TEST_PATH)

docs: ## Generate documentation using pdoc
	uv run pdoc -d google -o docs $(SOURCE_PATH)
