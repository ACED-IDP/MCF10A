"""Run flake8 tests"""

import subprocess
from typing import List


def test_conventions(directories: List[str] = ['util', 'transform', 'tests']):
    """Check python conventions on key directories"""
    failures = []
    for directory in directories:
        cmd_str = f"flake8 {directory}"
        completed = subprocess.run(cmd_str, shell=True)
        if completed.returncode != 0:
            _ = f"FAILURE: Python formatting and style for directory {directory}/"
            failures.append(_)
            print(_)

    assert len(failures) == 0, failures
