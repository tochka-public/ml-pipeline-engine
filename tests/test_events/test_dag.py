import typing as t
from unittest.mock import ANY
from uuid import UUID

from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.base_nodes.processors import ProcessorBase
from ml_pipeline_engine.chart import PipelineChart
from ml_pipeline_engine.context.dag import DAGPipelineContext
from ml_pipeline_engine.dag_builders.annotation import build_dag
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.types import NodeId
from ml_pipeline_engine.types import PipelineContextLike
from ml_pipeline_engine.types import PipelineResult


async def test_pipeline_chart_events_success(mocker, model_name_op):
    class SomeDataSourceNode(DataSource):
        name = 'some_datasource'

        def collect(self, x: int) -> int:
            return x * -1

    class SomeOutputNode(ProcessorBase):
        name = 'some_output'

        def process(self, num: Input(SomeDataSourceNode)) -> float:
            return num * 2.0

    class TestingEvents:
        async def on_pipeline_start(self, ctx: PipelineContextLike) -> None:
            ...

        async def on_pipeline_complete(self, ctx: PipelineContextLike, result: PipelineResult) -> None:
            ...

        async def on_node_start(self, ctx: PipelineContextLike, node_id: NodeId) -> None:
            ...

        async def on_node_complete(
            self,
            ctx: PipelineContextLike,
            node_id: NodeId,
            error: t.Optional[Exception],
        ) -> None:
            ...

    on_pipeline_start = mocker.spy(TestingEvents, 'on_pipeline_start')
    on_pipeline_complete = mocker.spy(TestingEvents, 'on_pipeline_complete')
    on_node_start = mocker.spy(TestingEvents, 'on_node_start')
    on_node_complete = mocker.spy(TestingEvents, 'on_node_complete')

    some_model_pipeline = PipelineChart(
        model_name=model_name_op,
        entrypoint=build_dag(input_node=SomeDataSourceNode, output_node=SomeOutputNode),
        event_managers=[
            TestingEvents,
        ],
    )

    await some_model_pipeline.run(pipeline_id=UUID('d18ff00c-3c43-4ed5-a755-13d6f89e6f44'), input_kwargs=dict(x=2))

    on_pipeline_start.assert_called_once()
    assert isinstance(on_pipeline_start.call_args.kwargs['ctx'], DAGPipelineContext)

    on_pipeline_complete.assert_called_once()
    assert on_pipeline_complete.call_args_list[0].kwargs == {
        'ctx': ANY,
        'result': PipelineResult(pipeline_id=UUID('d18ff00c-3c43-4ed5-a755-13d6f89e6f44'), value=-4.0, error=None),
    }
    assert isinstance(on_pipeline_complete.call_args.kwargs['ctx'], DAGPipelineContext)

    assert on_node_start.call_count == 2

    assert on_node_start.call_args_list[0].kwargs == {
        'ctx': ANY,
        'node_id': 'datasource__some_datasource',
    }

    assert on_node_start.call_args_list[1].kwargs == {
        'ctx': ANY,
        'node_id': 'processor__some_output',
    }

    assert on_node_complete.call_count == 2

    assert on_node_complete.call_args_list[0].kwargs == {
        'ctx': ANY,
        'node_id': 'datasource__some_datasource',
        'error': None,
    }

    assert on_node_complete.call_args_list[1].kwargs == {
        'ctx': ANY,
        'node_id': 'processor__some_output',
        'error': None,
    }


async def test_pipeline_chart_events_error(mocker, model_name_op):
    class SomeDataSourceNode(DataSource):
        name = 'some_datasource'

        async def collect(self, x: int) -> int:
            return x * -1

    class SomeOutputNode(ProcessorBase):
        name = 'some_output'

        def process(self, num: Input(SomeDataSourceNode)) -> float:
            return num / 0

    class TestingEvents:
        async def on_pipeline_start(self, ctx: PipelineContextLike) -> None:
            ...

        async def on_pipeline_complete(self, ctx: PipelineContextLike, result: PipelineResult) -> None:
            ...

        async def on_node_start(self, ctx: PipelineContextLike, node_id: NodeId) -> None:
            ...

        async def on_node_complete(
            self,
            ctx: PipelineContextLike,
            node_id: NodeId,
            error: t.Optional[Exception],
        ) -> None:
            ...

    on_pipeline_start = mocker.spy(TestingEvents, 'on_pipeline_start')
    on_pipeline_complete = mocker.spy(TestingEvents, 'on_pipeline_complete')
    on_node_start = mocker.spy(TestingEvents, 'on_node_start')
    on_node_complete = mocker.spy(TestingEvents, 'on_node_complete')

    some_model_pipeline = PipelineChart(
        model_name=model_name_op,
        entrypoint=build_dag(input_node=SomeDataSourceNode, output_node=SomeOutputNode),
        event_managers=[
            TestingEvents,
        ],
    )

    await some_model_pipeline.run(pipeline_id=UUID('c85d6f18-7b70-4ac9-bb49-04d784195cda'), input_kwargs=dict(x=2))

    on_pipeline_start.assert_called_once()
    assert isinstance(on_pipeline_start.call_args.kwargs['ctx'], DAGPipelineContext)

    on_pipeline_complete.assert_called_once()
    assert on_pipeline_complete.call_args_list[0].kwargs == {
        'ctx': ANY,
        'result': PipelineResult(pipeline_id=UUID('c85d6f18-7b70-4ac9-bb49-04d784195cda'), value=None, error=ANY),
    }
    assert isinstance(on_pipeline_complete.call_args_list[0].kwargs['result'].error, ZeroDivisionError)

    assert isinstance(on_pipeline_complete.call_args.kwargs['ctx'], DAGPipelineContext)

    assert on_node_start.call_count == 2

    assert on_node_start.call_args_list[0].kwargs == {
        'ctx': ANY,
        'node_id': 'datasource__some_datasource',
    }

    assert on_node_start.call_args_list[1].kwargs == {
        'ctx': ANY,
        'node_id': 'processor__some_output',
    }

    assert on_node_complete.call_count == 2

    assert on_node_complete.call_args_list[0].kwargs == {
        'ctx': ANY,
        'node_id': 'datasource__some_datasource',
        'error': None,
    }

    assert on_node_complete.call_args_list[1].kwargs == {
        'ctx': ANY,
        'node_id': 'processor__some_output',
        'error': ANY,
    }

    assert isinstance(on_node_complete.call_args_list[1].kwargs['error'], ZeroDivisionError)
