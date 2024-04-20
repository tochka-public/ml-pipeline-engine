import pytest


@pytest.fixture(autouse=True)
def default_retry_config(mocker) -> None:
    mocker.patch('ml_pipeline_engine.dag.DagRetryPolicy.attempts',
                 return_value=3, new_callable=mocker.PropertyMock)
    mocker.patch('ml_pipeline_engine.dag.DagRetryPolicy.delay',
                 return_value=0.3, new_callable=mocker.PropertyMock)
