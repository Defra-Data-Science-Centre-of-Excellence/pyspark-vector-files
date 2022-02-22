from enum import Enum
from typing import Tuple


class ConcurrencyStrategy(Enum):
    """Valid concurrency strategies."""

    FILES = "files"
    ROWS = "rows"


Chunk = Tuple[int, int]
Chunks = Tuple[Chunk, ...]
