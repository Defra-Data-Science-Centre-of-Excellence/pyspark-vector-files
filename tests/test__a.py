"""Deal with globbing both for standard and zipped files.

where multiple zips = ok, lots of files in single zip is not possible
Check the bit before the extension for an asterix - if it comes after - no deal

If after globbing a sequence of strings/paths are returned, then initiate building
ways to handle multiple...

Add other vsi types for different protocols - dictionary of extensions/vsi key/value
(handle None case)
Check for what comes after the . in the string and look it up in the dictionary
Don't worry about s3

"/path/to/*.ext" -> ("/path/to/file.ext", ...)
"/path/to/*.zip" -> ("/vsizip/path/to/file.zip", ...)
"https://path/to/*.ext" -> Error

22/11/2022:

- Expand the prefixer to handle other prefixes
- How does this fit in with the single path case?

Think about....
- need some logic in the prefixing how to introduce other extensions e.g. tar
- whats the pythonic way of doing a case statement? (homework)

"""
from contextlib import nullcontext as does_not_raise
from glob import glob
from pathlib import Path
from typing import Any, Sequence, Union

import pytest
from pytest import raises

# vsi lookup dict
lookup = {
        '.zip': '/vsizip/',
        '.gz': '/vsigzip/',
        #'.tar': '/vsitar/'
        #'.tar.gz': '/vsitar/' - deal with before individual tar/gz
    }

def process_path(
    file_path: Union[Path, str],
) -> Sequence[str]:
    """Process a given path."""
    _file_path = str(file_path)
    if not is_http(file_path):
        paths = glob_all(_file_path)
    else:
        paths = [_file_path]
    if not paths:
        raise ValueError("Pattern matching has not returned any paths.")
    prefixed_paths = prefix_paths(paths)
    return prefixed_paths


def is_http(
    file_path: Union[Path, str],
) -> bool:
    """Check if a path is a URL and whether its valid."""
    _path = str(file_path)
    if not _path.startswith("http"):
        return False
    elif "*" in _path:
        raise ValueError("URLs cannot contain wildcards.")
    elif isinstance(file_path, Path):
        raise ValueError("URLs must be provided as a string.")
    else:
        return True


def glob_all(
    file_path: str,
) -> Sequence[str]:
    """Glob a given path including matching wildcards."""
    return glob(file_path)


def prefix_path(
    file_path: str,
) -> str:
    """Prefix a path with the correct GDAL prefix, if required.
    Args:
        file_path str: A file path without required prefixes.
    Returns:
        str: A file path with required prefixes.
    """
    _file_path = file_path
    if file_path.startswith('http'):
        _file_path = f"/vsicurl/{_file_path}"
        
    s = Path(file_path).suffix
    prefix = lookup.get(s, '')
    _file_path = f"{prefix}{_file_path}"
    return _file_path


def prefix_paths(paths: Sequence[str]) -> Sequence[str]:
    """Run prefix_path over a list of paths."""
    return [prefix_path(path) for path in paths]


@pytest.mark.parametrize(
    argnames=(
        "file_path",
        "expected_exception",
        "expected_outputs",
    ),
    argvalues=(
        (
            "source_a/dataset_b/format_EXT_b/latest_b/file_1.ext",
            does_not_raise(),
            [
                "source_a/dataset_b/format_EXT_b/latest_b/file_1.ext",
            ],
        ),
        (
            Path("source_a/dataset_b/format_EXT_b/latest_b/file_1.ext"),
            does_not_raise(),
            [
                "source_a/dataset_b/format_EXT_b/latest_b/file_1.ext",
            ],
        ),
        (
            "source_a/dataset_b/format_ZIP_b/latest_b/file_1.zip",
            does_not_raise(),
            [
                "/vsizip/source_a/dataset_b/format_ZIP_b/latest_b/file_1.zip",
            ],
        ),
        (
            Path("source_a/dataset_b/format_ZIP_b/latest_b/file_1.zip"),
            does_not_raise(),
            [
                "/vsizip/source_a/dataset_b/format_ZIP_b/latest_b/file_1.zip",
            ],
        ),
        (
            "http://path/to/file.ext",
            does_not_raise(),
            [
                "/vsicurl/http://path/to/file.ext",
            ],
        ),
        (
            Path("http://path/to/file.ext"),
            raises(ValueError),
            None,
        ),
        (
            "http://path/to/file.zip",
            does_not_raise(),
            [
                "/vsizip//vsicurl/http://path/to/file.zip",
            ],
        ),
        (
            Path("http://path/to/file.zip"),
            raises(ValueError),
            None,
        ),
        (
            "https://path/to/file.ext",
            does_not_raise(),
            [
                "/vsicurl/https://path/to/file.ext",
            ],
        ),
        (
            Path("https://path/to/file.ext"),
            raises(ValueError),
            None,
        ),
        (
            "https://path/to/file.zip",
            does_not_raise(),
            [
                "/vsizip//vsicurl/https://path/to/file.zip",
            ],
        ),
        (
            Path("https://path/to/file.zip"),
            raises(ValueError),
            None,
        ),
        (
            "source_a/dataset_b/format_EXT_b/latest_b/file_*.ext",
            does_not_raise(),
            [
                "source_a/dataset_b/format_EXT_b/latest_b/file_1.ext",
                "source_a/dataset_b/format_EXT_b/latest_b/file_2.ext",
            ],
        ),
        (
            "source_a/dataset_*/format_EXT_*/latest_*/file_*.ext",
            does_not_raise(),
            [
                "source_a/dataset_b/format_EXT_b/latest_b/file_1.ext",
                "source_a/dataset_b/format_EXT_b/latest_b/file_2.ext",
                "source_a/dataset_c/format_EXT_c/latest_c/file_3.ext",
            ],
        ),
        (
            "source_a/dataset_b/format_EXT_b/latest_b/not_a_file_*.ext",
            raises(ValueError),
            None,
        ),
        (
            "https://path/to/*.zip",
            raises(ValueError),
            None,
        ),
    ),
    ids=(
        "Path as str",
        "Path as Path",
        "compressed path as string",
        "compressed path as Path",
        "network path as string",
        "network path as Path",
        "network compressed path as string",
        "network compressed path as Path",
        "secure network path as string",
        "secure network path as Path",
        "secure network compressed path as string",
        "secure network compressed path as Path",
        "Single wildcard",
        "Multiple wildcards",
        "Bad wildcard",
        "Http wildcard",
    ),
)
def test_process_paths(
    datadir: Path,
    expected_exception: Any,
    file_path: Union[Path, str],
    expected_outputs: Sequence[str],
) -> None:
    """."""
    with expected_exception:
        _path: Union[Path, str]
        if not str(file_path).startswith("http"):
            _path = datadir / file_path
        else:
            _path = file_path
        outputs = process_path(_path)
        _outputs = sorted(str(output).replace(f"{datadir}/", "") for output in outputs)
        assert _outputs == sorted(expected_outputs)
