from ml_pipeline_engine.base_nodes.datasources import DataSource
from ml_pipeline_engine.dag_builders.annotation.marks import Input
from ml_pipeline_engine.dag_builders.annotation.marks import InputGeneric
from ml_pipeline_engine.dag_builders.annotation.marks import InputOneOf
from ml_pipeline_engine.decorators import guard_datasource_error
from ml_pipeline_engine.node import build_node
from ml_pipeline_engine.types import NodeBase
from ml_pipeline_engine.types import NodeLike


class SomeInput(NodeBase):
    name = 'input'
    title = 'input'

    def process(self, base_num: int) -> int:
        return base_num


class FlDataSourceGeneric(DataSource):
    title = 'source'
    name = 'source'

    @guard_datasource_error()
    def collect(self, inp: InputGeneric(NodeLike)):
        raise Exception


FlDataSource = build_node(
    FlDataSourceGeneric,
    inp=Input(SomeInput),
)


class SomeFeatureGeneric(NodeBase):
    title = 'feature'

    def extract(self, fl_credit_history: InputGeneric(NodeLike)):
        # Не должно запускаться, так как fl_credit_history будет заполнено ошибкой.
        # Как следствие, эта нода обязана быть прощенной
        return len(fl_credit_history)


class SomeFeatureFallback(NodeBase):
    title = 'feature_fallback'

    def extract(self):
        return 777_777


SomeFeature = build_node(
    SomeFeatureGeneric,
    fl_credit_history=Input(FlDataSource),
    inp=Input(SomeInput),
)


class SomeMLModel(NodeBase):
    name = 'some_model'

    def predict(self, fl_credit_history_feature: InputOneOf([SomeFeature, SomeFeatureFallback])):
        return fl_credit_history_feature


async def test_fail_node(pipeline_context, build_dag, mocker):
    extract_patch = mocker.patch.object(SomeFeatureGeneric, 'extract')
    dag = build_dag(input_node=SomeInput, output_node=SomeMLModel)

    assert await dag.run(pipeline_context(base_num=10)) == 777_777
    assert extract_patch.call_count == 0
