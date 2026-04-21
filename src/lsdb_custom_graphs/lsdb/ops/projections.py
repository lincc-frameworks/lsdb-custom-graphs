from abc import ABC
from typing import Iterable


class Projection(ABC):
    pass


class ColumnProjection(Projection):
    def __init__(self, column_selector: str | Iterable[str]):
        self.column_selector = column_selector

    def __repr__(self):
        return f"ColumnProjection({self.column_name})"
