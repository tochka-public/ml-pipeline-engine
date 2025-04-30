# ML Pipeline Engine

Un-spaghetti and speed-up your data processing pipelines without usage of any complex DAG framework

## Key benefits

- Fast in-memory pipeline runtime. Suitable for online transaction processing
- No Graph DSL needed. Just use Pyhton type annotations to declare node's dependencies
- Strong topological analysis based pipeline execution engine with MVCC support
- Synchronous and asyncronous processor nodes supported
- Asyncronous pipeline interface enabled by default
- Simple but powerful control flow operators - no need to declare branches or node groups, just declare right dependencies
- Built-in pipeline visualizer
- Support for pipeline lifecycle events

## Table of Contents

- [Usage](#usage)
- [Development](#development)
    - [Environment setup](#environment-setup)


## Usage

```python
"""
main.py
"""

import asyncio
import time

from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.dag_builders.annotation import build_dag
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.node import ProcessorBase
from ml_pipeline_engine.parallelism import threads_pool_registry


# 1. Setup thread pool

threads_pool_registry.auto_init()


# 2. Define nodes and their dependencies


class InvertNumber(ProcessorBase):
    def process(self, num: float) -> float:
        return -num


class AsyncAddConst(ProcessorBase):
    async def process(self, num: Input(InvertNumber), const: float = 0.2) -> float:
        await asyncio.sleep(2)

        return num + const


class DoubleNumber(ProcessorBase):
    def process(self, num: Input(InvertNumber)) -> float:
        time.sleep(2)

        return num * 2


class AddNumbers(ProcessorBase):
    def process(self, num1: Input(AsyncAddConst), num2: Input(DoubleNumber)) -> float:
        return num1 + num2


# 3. Define pipeline

pipeline = PipelineChart(
    "example_pipeline",
    build_dag(input_node=InvertNumber, output_node=AddNumbers),
)

# 4. Run it


async def main():
    start = time.time()

    result = await pipeline.run(input_kwargs=dict(num=3.0))

    end = time.time()

    assert result.error is None
    assert result.value == -8.8

    # Execution engine used concurrency, basing on graph topology analysis,
    # so AsyncAddConst and DoubleNumber nodes were ran in parallel
    assert end - start < 2.1


if __name__ == "__main__":
    asyncio.run(main())
```

See additional usage examples in docs: [docs/examples/](docs/examples/).

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

Use `Python>=3.9` and the package manager [poetry](https://python-poetry.org/docs/#installing-manually) to install ml-pipeline-engine dependencies

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
