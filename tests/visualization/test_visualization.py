import functools
import json
import pathlib
import tempfile
from unittest.mock import ANY

import pytest
from click.testing import CliRunner

from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.cli import build_static
from ml_pipeline_engine.dag_builders.annotation.builder import build_dag
from ml_pipeline_engine.dag_builders.annotation.marks import Input, GenericInput, SwitchCase
from ml_pipeline_engine.node import build_node


class InvertNumber(DataSource):
    name = 'invert_number'
    verbose_name = 'Invert!'

    def collect(self, num: float):
        return -num


class AddConst(ProcessorBase):
    name = 'add_const'
    verbose_name = 'Add!'

    def process(self, num: Input(InvertNumber), const: float = 0.1):
        return num + const


class DoubleNumber(ProcessorBase):
    name = 'double_number'
    verbose_name = 'Double!'

    def process(self, num: Input(AddConst)) -> float:
        return num * 2


class GenericAnotherFeature(ProcessorBase):
    name = 'another_feature'

    def process(self, inp: GenericInput(DoubleNumber)) -> float:
        return inp + 1


NoGenericFeature = build_node(GenericAnotherFeature, inp=Input(DoubleNumber))


class Const(ProcessorBase):
    def process(self, number: Input(NoGenericFeature)):
        return 10.0


class SwitchNode(ProcessorBase):
    def process(self):
        return 'const'


SomeSwitchCase = SwitchCase(
    switch=SwitchNode,
    cases=[
        ('const', Const),
    ],
)


class JustNode(ProcessorBase):
    def process(self, num: SomeSwitchCase):
        return 1


dag = build_dag(input_node=InvertNumber, output_node=JustNode)
runner = CliRunner()


@pytest.mark.parametrize(
    'call_func',
    (
        lambda target_dir: (
            functools.partial(
                dag.visualize,
                name='Dag-for-test',
                verbose_name='Dag - verbose name!',
                target_dir=target_dir,
            )
        ),
        lambda target_dir: (
            functools.partial(
                runner.invoke,
                build_static,
                [
                    '--dag_path', 'tests.visualization.test_visualization:dag',
                    '--dag_name', 'Dag-for-test',
                    '--dag_verbose_name', 'Dag - verbose name!',
                    '--target_dir', str(target_dir),
                ]
            )
        ),
    ),
)
async def test_basic(call_func):

    with tempfile.TemporaryDirectory() as tmp_dir:
        target = pathlib.Path(tmp_dir) / 'true-target'

        call_func(target)()

        saved_config = (target / 'data.js').read_text()
        saved_config = json.loads(saved_config.replace('data = ', ''))

        assert saved_config == {
            'attributes': {
                'edgesep': 60,
                'name': 'Dag-for-test',
                'ranksep': 700,
                'repo_link': None,
                'verbose_name': 'Dag - verbose name!'
            },
            'edges': [
                {
                    'id': ANY,
                    'source': ANY,
                    'target': 'processor__tests_visualization_test_visualization_JustNode'
                },
                {
                    'id': ANY,
                    'source': 'processor__tests_visualization_test_visualization_SwitchNode',
                    'target': ANY,
                },
                {
                    'id': ANY,
                    'source': 'processor__tests_visualization_test_visualization_Const',
                    'target': ANY,
                },
                {
                    'id': 'processor__another_feature->processor__tests_visualization_test_visualization_Const',
                    'source': 'processor__another_feature',
                    'target': 'processor__tests_visualization_test_visualization_Const'
                },
                {
                    'id': 'processor__double_number->processor__another_feature',
                    'source': 'processor__double_number',
                    'target': 'processor__another_feature'
                },
                {
                    'id': 'processor__add_const->processor__double_number',
                    'source': 'processor__add_const',
                    'target': 'processor__double_number'
                },
                {
                    'id': 'datasource__invert_number->processor__add_const',
                    'source': 'datasource__invert_number',
                    'target': 'processor__add_const'
                },
                {
                    'id': 'datasource__invert_number->processor__tests_visualization_test_visualization_SwitchNode',
                    'source': 'datasource__invert_number',
                    'target': 'processor__tests_visualization_test_visualization_SwitchNode'
                }
            ],
            'node_types': {
                'datasource': {
                    'hex_bgr_color': None,
                    'name': 'datasource'
                },
                'processor': {
                    'hex_bgr_color': None,
                    'name': 'processor'
                },
                'switch': {
                    'hex_bgr_color': None,
                    'name': 'switch'
                }
            },
            'nodes': [
                {
                    'data': None,
                    'id': ANY,
                    'is_generic': False,
                    'is_virtual': True,
                    'type': None
                },
                {
                    'data': {
                        'code_source': 'tests/visualization/test_visualization.py#L57',
                        'doc': 'Базовый класс для обработчиков общего назначения',
                        'name': None,
                        'verbose_name': None
                    },
                    'id': 'processor__tests_visualization_test_visualization_SwitchNode',
                    'is_generic': False,
                    'is_virtual': False,
                    'type': 'processor'
                },
                {
                    'data': {
                        'code_source': 'tests/visualization/test_visualization.py#L52',
                        'doc': 'Базовый класс для обработчиков общего назначения',
                        'name': None,
                        'verbose_name': None
                    },
                    'id': 'processor__tests_visualization_test_visualization_Const',
                    'is_generic': False,
                    'is_virtual': False,
                    'type': 'processor'
                },
                {
                    'data': {
                        'code_source': 'tests/visualization/test_visualization.py#L70',
                        'doc': 'Базовый класс для обработчиков общего назначения',
                        'name': None,
                        'verbose_name': None
                    },
                    'id': 'processor__tests_visualization_test_visualization_JustNode',
                    'is_generic': False,
                    'is_virtual': False,
                    'type': 'processor'
                },
                {
                    'data': {
                        'code_source': 'ml_pipeline_engine/node/node.py#L42',  # Line for the real source!
                        'doc': 'Базовый класс для обработчиков общего назначения',
                        'name': 'another_feature',
                        'verbose_name': None
                    },
                    'id': 'processor__another_feature',
                    'is_generic': True,
                    'is_virtual': False,
                    'type': 'processor'
                },
                {
                    'data': {
                        'code_source': 'tests/visualization/test_visualization.py#L34',
                        'doc': 'Базовый класс для обработчиков общего назначения',
                        'name': 'double_number',
                        'verbose_name': 'Double!'
                    },
                    'id': 'processor__double_number',
                    'is_generic': False,
                    'is_virtual': False,
                    'type': 'processor'
                },
                {
                    'data': {
                        'code_source': 'tests/visualization/test_visualization.py#L26',
                        'doc': 'Базовый класс для обработчиков общего назначения',
                        'name': 'add_const',
                        'verbose_name': 'Add!'
                    },
                    'id': 'processor__add_const',
                    'is_generic': False,
                    'is_virtual': False,
                    'type': 'processor'
                },
                {
                    'data': {
                        'code_source': 'tests/visualization/test_visualization.py#L18',
                        'doc': 'Базовый класс для источников данных',
                        'name': 'invert_number',
                        'verbose_name': 'Invert!'
                    },
                    'id': 'datasource__invert_number',
                    'is_generic': False,
                    'is_virtual': False,
                    'type': 'datasource'
                }
            ]
        }

        assert (target / 'index.html').exists()
