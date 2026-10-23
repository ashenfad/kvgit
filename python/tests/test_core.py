from importlib.metadata import version

from kvgit import _core


def test_extension_matches_package_version():
    assert _core.version() == version("kvgit")
