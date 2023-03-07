import pytest
from dataexec.assets import TextAsset
from dataexec.steps import Step
from dataexec import types, errors


def get_asset(error=False):
    if error:
        raise NameError("func failed")

    asset = TextAsset.from_location("tests/text_asset.txt")
    return asset


async def aio_get_asset(error=False):
    if error:
        raise NameError("func failed")

    asset = TextAsset.from_location("tests/text_asset.txt")
    return asset


def get_asset_list(error=False):
    if error:
        raise NameError("func failed")

    asset = TextAsset.from_location("tests/text_asset.txt")
    return [asset, asset]


def test_steps():
    step = Step("test", get_asset)
    result = step({})
    assert isinstance(result, types.Output)
    assert result.status == types.ExecStatus.completed


def test_steps_error():
    step = Step("test", get_asset)
    with pytest.raises(errors.StepExecutionError):
        step(error=True)


@pytest.mark.asyncio
async def test_steps_async():
    step = Step("test", aio_get_asset, is_async=True)
    async with step.execute() as result:
        assert isinstance(result, types.Output)

    assert result.status == types.ExecStatus.completed
