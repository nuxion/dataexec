from pathlib import Path
import tempfile
from dataexec.assets import TextAsset, copy_asset
from dataexec.types import Asset


def test_assets_open():
    asset = TextAsset.from_location("tests/text_asset.txt")
    assert isinstance(asset, Asset)
    assert isinstance(asset, TextAsset)
    assert asset.it_exist()
    assert asset.raw.strip() == "testing_asset"


def test_assets_copy():
    asset = TextAsset.from_location("tests/text_asset.txt")

    tmp = tempfile.NamedTemporaryFile()
    new_asset = copy_asset(asset, tmp.name)
    new_asset.write()
    assert id(asset) != id(new_asset)
    assert Path(tmp.name).is_file()
