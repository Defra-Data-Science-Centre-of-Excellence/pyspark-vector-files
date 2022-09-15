"""Get paths."""
from pathlib import Path
from tarfile import is_tarfile
from zipfile import is_zipfile


def _is_gzip(path: Path) -> bool:

    with open(path, "rb") as gzipped_file:
        return gzipped_file.read(3) == b"\x1f\x8b\x08"


def _get_path_type(path: str) -> str:
    _path = Path(path)
    if _path.is_dir():
        return "folder"
    elif (is_tarfile(_path) and _is_gzip(_path)) or is_zipfile(_path):
        return "compressed folder"
    elif is_tarfile(_path):
        return "archive file"
    elif _is_gzip(_path):
        return "compressed file"
    elif _path.is_file():
        return "normal file"
    else:
        raise ValueError(f"Unable to determine type of {_path}")
