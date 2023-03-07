import tempfile
from dataexec.workflows import Workflow
import pytest
from dataexec.assets import TextAsset, copy_asset
from dataexec.steps import Step
from dataexec import types, errors


def get_asset(txt: str, error=False):
    if error:
        raise NameError("func failed")

    asset = TextAsset.from_location(txt)
    return asset


def process_text(asset: TextAsset, error=False):
    # asset.raw
    if error:
        raise NameError("func failed")
    tmp = tempfile.NamedTemporaryFile()
    new_asset = copy_asset(asset, tmp.name)
    new_asset._raw = "modified asset"
    new_asset.write()
    return new_asset


def test_workflow_run():
    txt = "tests/text_asset.txt"
    w = Workflow(
        steps=[
            Step("get_asset", get_asset, params={"txt": txt}),
            Step("transform", process_text),
        ]
    )
    result = w.run()
    assert len(w.exec_log) == 2
    assert isinstance(result, types.Output)
    assert result.assets[0].raw == "modified asset"


def test_workflow_run_with_params():
    txt = "tests/text_asset.txt"
    w = Workflow(
        steps=[
            Step("get_asset", get_asset),
            Step("transform", process_text),
        ]
    )
    result = w.run(txt=txt)
    assert len(w.exec_log) == 2
    assert isinstance(result, types.Output)
    assert result.assets[0].raw == "modified asset"


def test_workflow_run_error():
    txt = "tests/text_asset.txt"
    w = Workflow(
        steps=[
            Step("get_asset", get_asset),
            Step("transform", process_text),
        ]
    )
    with pytest.raises(errors.StepExecutionError):
        w.run(txt=txt, error=True)
