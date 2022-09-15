"""Tests for get paths."""
from pathlib import Path

import pytest
from _pytest.fixtures import FixtureRequest
from pytest import fixture

from pyspark_vector_files._paths import _get_path_type


@fixture
def normal_file(shared_datadir: Path) -> str:
    """Path to a normal file."""
    _path = shared_datadir / "normal_file.txt"
    return str(_path)


@fixture
def gzipped_file(shared_datadir: Path) -> str:
    """Path to a gzipped file."""
    _path = shared_datadir / "normal_file.txt.gz"
    return str(_path)


@fixture
def gzipped_tar_file(shared_datadir: Path) -> str:
    """Path to a gzipped tar file."""
    _path = shared_datadir / "folder.tar.gz"
    return str(_path)


@fixture
def tar_file(shared_datadir: Path) -> str:
    """Path to a tar file."""
    _path = shared_datadir / "folder.tar"
    return str(_path)


@fixture
def zip_file(shared_datadir: Path) -> str:
    """Path to a zipped file."""
    _path = shared_datadir / "folder.zip"
    return str(_path)


@fixture
def folder(shared_datadir: Path) -> str:
    """Path to a folder."""
    _path = shared_datadir / "folder"
    return str(_path)


@pytest.mark.parametrize(
    argnames=("path", "expected_path_type"),
    argvalues=(
        ("normal_file", "normal file"),
        ("gzipped_file", "compressed file"),
        ("gzipped_tar_file", "compressed folder"),
        ("tar_file", "archive file"),
        ("zip_file", "compressed folder"),
        ("folder", "folder"),
    ),
    ids=(
        "normal file",
        "gzipped file",
        "gzipped tar file",
        "tar file",
        "zipped file",
        "folder",
    ),
)
def test_get_path_type(
    path: str, request: FixtureRequest, expected_path_type: str
) -> None:
    """Identifies the correct type."""
    _path = request.getfixturevalue(path)
    path_type = _get_path_type(_path)
    assert path_type == expected_path_type
