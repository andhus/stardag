import sys
from pathlib import Path

import pytest

EXAMPLES_DIR = (Path(__file__).parents[3] / "examples").absolute()


@pytest.fixture(scope="module")
def examples_in_sys_path():
    sys.path.append(EXAMPLES_DIR.as_posix())
    try:
        yield
    finally:
        sys.path.remove(EXAMPLES_DIR.as_posix())
