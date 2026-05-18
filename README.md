# lubrikit

[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![mypy](https://img.shields.io/badge/mypy-checked-blue)](http://mypy-lang.org/)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-312/)
[![Deploy Documentation](https://github.com/Lubricate-AI/lubrikit/actions/workflows/docs.yml/badge.svg)](https://github.com/Lubricate-AI/lubrikit/actions/workflows/docs.yml)

**lubrikit** is a Python SDK for building **collection, ingestion, and processing**
data pipelines. It provides a small set of composable primitives — pipelines,
connectors, and storage clients — that let you stand up reproducible data
workflows without rewriting plumbing for every new source.

> 📖 Full documentation: <https://lubricate-ai.github.io/lubrikit/>

---

## Table of contents

- [Why lubrikit?](#why-lubrikit)
- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Project structure](#project-structure)
- [Development](#development)
- [Versioning](#versioning)
- [License](#license)

## Why lubrikit?

Most data-pipeline code is the same few moving parts wired up differently every
time: pull bytes from a source, push them into a storage layer, retry on
transient failures, and keep enough metadata to know what's new. lubrikit
factors those moving parts into reusable classes so each new source is
**configuration** instead of yet another bespoke script.

The SDK is organized around the three stages of a typical pipeline:

- **Extract** — pull raw bytes from external systems into a *landing* layer
- **Load** — read landed files and write them into structured storage
- **Transform** — read structured storage and write derived datasets back to it

Today the **extract** stage is the most fully fleshed out; **load** and
**transform** are intentionally lightweight scaffolds you can extend.

## Features

- 🔌 **Pluggable connectors** for HTTP endpoints and the Google Drive API,
  with a `BaseConnector` you can subclass for new sources.
- 🔁 **Retry-with-backoff decorator** (`lubrikit.utils.retry`) with
  configurable max attempts, exponential backoff, and jitter.
- 🗂️ **Storage abstraction** — `StorageClient` and `FileMetadata` types model
  the landing/staging/processed layers consistently across local disk and S3
  (via [`s3fs`](https://github.com/fsspec/s3fs)).
- 🧾 **Conditional downloads** — connectors honor `ETag`, `Last-Modified`, and
  `Content-Length` so unchanged resources are skipped.
- 🛡️ **Typed contracts** — `TypedDict` metadata and Pydantic models for
  connector configs, fully type-checked with `mypy --strict`.
- 🪵 **Library-friendly logging** — quiet by default (a `NullHandler` is
  attached); set `LUBRIKIT_DEV=1` to enable verbose stream logging during
  local development.

## Installation

lubrikit requires **Python 3.12+**.

```bash
# With uv (recommended)
uv add lubrikit

# With pip
pip install lubrikit
```

Until lubrikit is published to PyPI, install directly from the repo:

```bash
uv add "git+https://github.com/Lubricate-AI/lubrikit.git"
```

## Quick start

The most common entry point is `ExtractPipeline`, which composes a connector
and a storage client from a single metadata dict.

```python
from lubrikit.extract.pipeline import ExtractPipeline

metadata = {
    "source_name": "example",
    "prefix": "raw",
    "connector": "HTTPConnector",
    "connector_config": {
        "method": "GET",
        "url": "https://api.example.com/v1/widgets.json",
    },
    "retry_config": {
        "max_retries": 3,
        "base_delay": 1.0,
        "backoff_factor": 2.0,
        "timeout": 30,
    },
    "headers_cache": {},  # populated on subsequent runs with ETag/Last-Modified
}

ExtractPipeline(metadata).run()
```

The pipeline:

1. Instantiates `HTTPConnector` from `connector_config`.
2. Issues the request with retry-with-backoff on connection/timeout/HTTP errors.
3. Skips the download if `ETag` / `Last-Modified` indicate the resource is unchanged.
4. Streams the response into the landing layer via `ExtractStorageClient`.

To wrap your own function with the same retry behavior:

```python
import requests
from lubrikit.utils.retry import retry_with_backoff

@retry_with_backoff(
    max_retries=5,
    base_delay=1.0,
    backoff_factor=2.0,
    retriable_exceptions=(requests.exceptions.ConnectionError,),
)
def fetch_widgets() -> dict:
    return requests.get("https://api.example.com/widgets").json()
```

## Project structure

```
lubrikit/
├── base/          # abstract Pipeline + storage primitives (FileMode, Layer)
├── extract/       # ExtractPipeline + HTTP/GoogleDrive connectors + storage
├── load/          # placeholder for the load stage
├── transform/     # placeholder for the transform stage
└── utils/
    └── retry/     # retry_with_backoff decorator + RetryConfig
```

## Development

```bash
# Set up the dev environment
make install        # uv sync --extra dev

# Run the full quality gate
make lint           # ruff + typos + yamllint + mypy
make test           # pytest with strict markers
make coverage       # pytest + term-missing coverage

# Auto-format and auto-fix imports
make format

# Build API docs locally
make docs
```

Enable verbose logging while iterating:

```bash
LUBRIKIT_DEV=1 python -m your_script
```

## Versioning

This project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Until the `1.0.0` release, the public API may change between minor versions.

## License

Released under the **MIT License**. See [`pyproject.toml`](pyproject.toml) for the
canonical license declaration.
