# ML Pipeline Engine

Графовый движок для конвейеров ML-моделей

## Table of Contents

- [Usage](#usage)
- [Development](#development)
    - [Environment setup](#environment-setup)


## Usage

To create a pipeline:
1. Define classes that represent nodes
2. Connect nodes by defining the parent node(-s) or no parent for each node


Примеры использования описаны в файле [docs/usage_examples.md](docs/usage_examples.md)

## Development

### Environment setup

Clone the project
```bash
git clone https://github.com/tochka-public/ml-pipeline-engine.git
```

Go to the project directory

```bash
cd ml-pipeline-engine
```

Use `Python>=3.8` and the package manager [poetry](https://python-poetry.org/docs/#installing-manually) to install ml-pipeline-engine dependencies

```bash
poetry install --no-root
```

For further contribution, use [pre-commit](https://pre-commit.com/#intro) hooks to maintain consistent code format

```bash
pre-commit install -f --hook-type pre-commit --hook-type pre-push
```

Run tests
```bash
python -m pytest tests
```
