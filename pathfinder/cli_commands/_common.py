"""Small parser helpers shared by Pathfinder command families."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Protocol


class PayloadPrinter(Protocol):
    def __call__(self, payload: object, *, compact: bool) -> int: ...


def add_required_path(
    command: argparse.ArgumentParser,
    option: str,
) -> None:
    command.add_argument(option, type=Path, required=True)


def add_compact(command: argparse.ArgumentParser) -> None:
    command.add_argument("--compact", action="store_true")


def add_output_dir(command: argparse.ArgumentParser) -> None:
    add_required_path(command, "--output-dir")
