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
"""
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from typing import Any, Union

import pytest
from pytest import raises


def path_single_file(
    file_path: Union[Path, str],
) -> Union[Path, str]:
    """#TODO.

    Args:
        file_path (Union[Path, str]): #TODO.

    Raises:
        ValueError: #TODO,

    Returns:
        Union[Path, str]: #TODO.
    """
    if isinstance(file_path, str) and file_path.startswith("http"):

        if file_path.endswith(".zip"):

            return f"/vsizip/vsicurl/{file_path}"

        return f"/vsicurl/{file_path}"

    if isinstance(file_path, Path) and str(file_path).startswith("http"):
        raise ValueError("URLs must be provided as a string.")

    p = Path(file_path)
    s = p.suffix

    if s == ".zip":
        return Path(f"/vsizip/{file_path}")

    return p


@pytest.mark.parametrize(
    argnames=(
        "file_path",
        "expected_exception",
        "expected_output",
    ),
    argvalues=(
        ("/path/to/file.ext", does_not_raise(), Path("/path/to/file.ext")),
        (Path("/path/to/file.ext"), does_not_raise(), Path("/path/to/file.ext")),
        ("/path/to/file.zip", does_not_raise(), Path("/vsizip/path/to/file.zip")),
        (Path("/path/to/file.zip"), does_not_raise(), Path("/vsizip/path/to/file.zip")),
        (
            "http://path/to/file.ext",
            does_not_raise(),
            "/vsicurl/http://path/to/file.ext",
        ),
        (
            "http://path/to/file.zip",
            does_not_raise(),
            "/vsizip/vsicurl/http://path/to/file.zip",
        ),
        (Path("http://path/to/file.ext"), raises(ValueError), None),
        (
            "https://path/to/file.ext",
            does_not_raise(),
            "/vsicurl/https://path/to/file.ext",
        ),
        (
            "https://path/to/file.zip",
            does_not_raise(),
            "/vsizip/vsicurl/https://path/to/file.zip",
        ),
        (Path("https://path/to/file.ext"), raises(ValueError), None),
    ),
    ids=(
        "path as string",
        "path as Path",
        "compressed path as string",
        "compressed path as Path",
        "network path as string",
        "network compressed path as string",
        "network path as Path",
        "secure network path as string",
        "secure network compressed path as string",
        "secure network path as Path",
    ),
)
def test_path_single_file(
    file_path: Union[Path, str],
    expected_exception: Any,
    expected_output: Union[Path, str],
) -> None:
    """??."""
    with expected_exception:
        output = path_single_file(file_path)
        assert output == expected_output
