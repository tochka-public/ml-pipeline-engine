# ML Pipeline Engine

Графовый движок для конвейеров ML-моделей

## Table of Contents

- [Usage](#usage)
  - [Что нужно, чтобы сделать свой пайплайн?](#что-нужно-чтобы-сделать-свой-пайплайн)
  - [Поддерживаемые типы узлов](#поддерживаемые-типы-узлов)
- [Development](#development)
    - [Environment setup](#environment-setup)


## Usage
### Что нужно, чтобы сделать свой пайплайн?

1. Написать классы узлов
2. Связать узлы посредством указания зависимости


### Поддерживаемые типы узлов

[Протоколы](ml_pipeline_engine/types.py)

1. [DataSource](ml_pipeline_engine/base_nodes/datasources.py)
2. [FeatureBase](ml_pipeline_engine/base_nodes/feature.py)
3. [MLModelBase](ml_pipeline_engine/base_nodes/ml_model.py)
4. [ProcessorBase](ml_pipeline_engine/base_nodes/processors.py)
5. [FeatureVectorizerBase](ml_pipeline_engine/base_nodes/vectorizer.py)

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
