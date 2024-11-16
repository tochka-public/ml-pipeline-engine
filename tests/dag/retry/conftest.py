import pytest
import pytest_mock


@pytest.fixture(autouse=True)
def default_retry_config(mocker: pytest_mock.MockerFixture) -> None:
    mocker.patch(
        'ml_pipeline_engine.dag.NodeRetryPolicy.attempts',
        return_value=3,
        new_callable=mocker.PropertyMock,
    )
    mocker.patch(
        'ml_pipeline_engine.dag.NodeRetryPolicy.delay',
        return_value=0.3,
        new_callable=mocker.PropertyMock,
    )
