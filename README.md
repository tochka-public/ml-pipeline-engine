# ML Pipeline Engine

Графовый движок для конвейеров ML-моделей


## Что нужно, чтобы сделать свой пайплайн?

1. Написать классы узлов
2. Связать узлы посредством указания зависимости


## Поддерживаемые типы узлов

[Протоколы](ml_pipeline_engine/types.py)

1. [DataSource](ml_pipeline_engine/base_nodes/datasources.py)
2. [FeatureBase](ml_pipeline_engine/base_nodes/feature.py)
3. [MLModelBase](ml_pipeline_engine/base_nodes/ml_model.py)
4. [ProcessorBase](ml_pipeline_engine/base_nodes/processors.py)
5. [FeatureVectorizerBase](ml_pipeline_engine/base_nodes/vectorizer.py)


## Примеры

#### Простейший пример на основе функций

```python
def invert_number(num: float):
    return -num

def add_const(num: Input(invert_number), const: float = 0.1):
    return num + const

def double_number(num: Input(add_const)):
    return num * 2

build_dag(input_node=invert_number, output_node=double_number).run(pipeline_context(num=2.5))
```

#### Пример, который реализует переключение нод по условию (Аналог switch)

```python
 def ident(num: float):
     return num

 def switch_node(num: Input(ident)):
     if num < 0.0:
         return 'invert'
     if num == 0.0:
         return 'const'
     if num == 1.0:
         return 'double'
     return 'add_sub_chain'

 def const_noinput():
     return 10.0

 def add_100(num: Input(ident)):
     return num + 100

 def add_some_const(num: Input(ident), const: float = 9.0):
     return num + const

 def invert_number(num: Input(ident)):
     return -num

 def sub_ident(num: Input(add_some_const), num_ident: Input(ident)):
     return num - num_ident

 def double_number(num: Input(ident)):
     return num * 2

 SomeSwitchCase = SwitchCase(
     switch=switch_node,
     cases=[
         ('const', const_noinput),
         ('double', double_number),
         ('invert', invert_number),
         ('add_sub_chain', sub_ident),
     ],
 )

 def case_node(num: SomeSwitchCase, num2: Input(ident), num3: Input(add_100)):
     return num + num2 + num3

 def out(num: Input(case_node)):
     return num

build_dag(input_node=ident, output_node=out).run(pipeline_context(num=input_num))
```

#### Пример с использованием классов как узлов графа

```python
class SomeInput(NodeBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }

class SomeDataSource(NodeBase):
    name = 'some_data_source'

    def collect(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100

class SomeFeature(NodeBase):
    name = 'some_feature'

    def extract(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10

class SomeVectorizer(NodeBase):
    name = 'some_vectorizer'

    def vectorize(self, feature_value: Input(SomeFeature)) -> int:
        return feature_value + 20

class SomeMLModel(NodeBase):
    name = 'some_model'

    def predict(self, vec_value: Input(SomeVectorizer)):
        return (vec_value + 30) / 100


build_dag(input_node=SomeInput, output_node=SomeMLModel).run(pipeline_context(base_num=10, other_num=5))
```


#### Пример с использованием Generic-нод, которые под собой хранят общее поведение и не зависимы от конкретной модели

```python
class SomeInput(NodeBase):
    name = 'input'

    def process(self, base_num: int, other_num: int) -> dict:
        return {
            'base_num': base_num,
            'other_num': other_num,
        }

class SomeDataSource(NodeBase):
    name = 'some_data_source'

    def collect(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100

class SomeCommonFeature(NodeBase):
    name = 'some_feature'

    def extract(self, ds_value: Input(SomeDataSource), inp: Input(SomeInput)) -> int:
        return ds_value + inp['other_num'] + 10

# Пример Generic-ноды
class GenericVectorizer(NodeBase):
    name = 'some_vectorizer'

    def vectorize(self, feature_value: InputGeneric(NodeLike)) -> int:
        return feature_value + 20

class AnotherFeature(NodeBase):
    name = 'another_feature'

    def extract(self, inp: Input(SomeInput)) -> int:
        return inp['base_num'] + 100_000

# Первый переопределенный подграф
SomeParticularVectorizer = build_node(  # noqa
    GenericVectorizer,
    feature_value=Input(SomeCommonFeature),
)

# Второй переопределенный подграф
AnotherParticularVectorizer = build_node(  # noqa
    GenericVectorizer,
    feature_value=Input(AnotherFeature),
)

class SomeMLModel(NodeBase):
    name = 'some_model'

    def predict(self, vec_value: Input(SomeParticularVectorizer)):
        return (vec_value + 30) / 100

class AnotherMlModel(NodeBase):
    name = 'another_model'

    def predict(self, vec_value: Input(AnotherParticularVectorizer)):
        return (vec_value + 30) / 100

# Первый граф
build_dag(input_node=SomeInput, output_node=SomeMLModel).run(pipeline_context(base_num=10, other_num=5))

# Второй граф
build_dag(input_node=SomeInput, output_node=AnotherMlModel).run(pipeline_context(base_num=10, other_num=5))
```

#### Пример указания ретрая

```python
class HierarchyException(Exception):
    pass


class SecondHierarchyException(Exception):
    pass


class SomeMlModel(NodeBase):

    node_type = 'ml_model'
    delay = 1.1
    attempts = 5
    exceptions = (SecondHierarchyException, )

    def predict(self, *args, **kwargs):
        ...
```


#### Пример запуска чарта с хранилищем артефактов

```python
from ml_pipeline_engine.artifact_store.store.filesystem import FileSystemArtifactStore


def invert_number(num: float):
    return -num

def add_const(num: Input(invert_number), const: float = 0.1):
    return num + const

def double_number(num: Input(add_const)):
    return num * 2

dag = build_dag(input_node=invert_number, output_node=double_number)

chart = PipelineChart(
   model_name='name',
   entrypoint=dag,
   # Тут может быть указан артефакт стор, который пишет данные в S3
   artifact_store=... if not is_local() else FileSystemArtifactStore,
   # Добавление эвентов позволяет отслеживать полезную нагрузку, результаты и ошибки узлов для построения графиков
   event_managers=[DatabaseEventManager],
)

result = chart.run(
   input_kwargs=dict(
      ...,
   ),
)

```

## Визуализация пайплайна

```bash
ml_pipeline_engine build-static --dag_path 'ml_pipeline_engine.visualization.sample:sample_dag' --target_dir ./public
```

## Установка либы для разработки

### Первоначальная настройка

1. Устанавливаем Python>=3.8

2. Устанавливаем зависимости для тестирования:

    ```bash
    pip3 install -e ".[tests]"
    ```

3. Подключаем pre-commit hooks

    ```bash
    pre-commit install -f
    ```

4. Запуск тестов

    ```bash
    python -m pytest tests
    ```
