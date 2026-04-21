from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Callable, Self

import pandas as pd

from lsdb_custom_graphs.lsdb.ops.projections import Projection, ColumnProjection


def projection_handler(projection_type: type[Projection]):
    """Decorator to register a method as a handler for a given Projection type."""

    def decorator(method: Callable) -> Callable:
        method._projection_type = projection_type
        return method

    return decorator


def projection_emitter(projection_type: type[Projection]):
    """Decorator to mark a method as a source of a given Projection type to push to input operations."""

    def decorator(method: Callable) -> Callable:
        method._emits_projection_type = projection_type
        return method

    return decorator


class HealpixGraph:
    """Task Graph where each node corresponds to a HEALPix pixel"""

    def __init__(self, graph: dict, pixel_to_key_map: dict):
        self.graph = graph
        self.pixel_to_key_map = pixel_to_key_map

    @property
    def keys(self):
        return list(self.pixel_to_key_map.values())


class _OperationBase(ABC):
    projection_handlers: dict[type[Projection], Callable] = {}
    projection_emitters: dict[type[Projection], Callable] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls.projection_handlers = dict(cls.projection_handlers)
        cls.projection_emitters = dict(cls.projection_emitters)
        new_projection_handlers = {}
        new_projection_emitters = {}
        for method in vars(cls).values():
            if not callable(method):
                continue
            if hasattr(method, "_projection_type"):
                projection_type = method._projection_type
                if projection_type in new_projection_handlers:
                    raise ValueError(
                        f"Multiple handlers for projection type {projection_type} in class {cls.__name__}")
                new_projection_handlers[projection_type] = method
            if hasattr(method, "_emits_projection_type"):
                projection_type = method._emits_projection_type
                if projection_type in new_projection_emitters:
                    raise ValueError(
                        f"Multiple emitters for projection type {projection_type} in class {cls.__name__}")
                new_projection_emitters[projection_type] = method
        cls.projection_handlers.update(new_projection_handlers)
        cls.projection_emitters.update(new_projection_emitters)


class Operation(_OperationBase):
    allow_column_projection_passthrough = False

    @property
    @abstractmethod
    def name(self) -> str:
        """The name of the Task"""
        pass

    @property
    @abstractmethod
    def key_name(self) -> str:
        pass

    @abstractmethod
    def build(self) -> HealpixGraph:
        """Returns the task graph for this operation, where each node corresponds to a HEALPix pixel"""
        pass

    @property
    @abstractmethod
    def meta(self) -> pd.DataFrame:
        """Returns the metadata for the output of this operation"""
        pass

    @property
    @abstractmethod
    def dependencies(self) -> list[Self]:
        """Returns the list of input operations for this operation"""
        pass

    @abstractmethod
    def replace_dependencies(self, dependencies: list[Self]) -> Self:
        """Returns the metadata for the output of this operation"""
        pass

    def handle_projections(self, projections: list[Projection]) -> Self:
        """Apply a list of projections to this operation, returning a new Operation with the projections applied."""
        op = self
        for projection_type in self.projection_emitters:
            if not any(isinstance(projection, projection_type) for projection in projections):
                projections = projections + [self.projection_emitters[projection_type](self)]
        propagated_projections = []
        for projection in projections:
            op, proj = op.handle_projection(projection)
            if proj is not None:
                propagated_projections.append(proj)
        if len(propagated_projections) > 0:
            deps = self.dependencies
            deps = [dep.handle_projections(propagated_projections) for dep in deps]
            op = op.replace_dependencies(deps)
        return op

    def handle_projection(self, projection: Projection) -> tuple[Self, Projection | None]:
        """Apply a projection to this operation, returning a new Operation with the projection applied."""
        handler = self.projection_handlers.get(type(projection))
        if handler is None:
            return self, None
        return handler(self, projection)

    def optimize(self) -> Self:
        return self.handle_projections([])

    def __repr__(self):
        return self.name

    @projection_handler(ColumnProjection)
    def handle_column_projection(self, projection: ColumnProjection) -> tuple[Self, ColumnProjection]:
        if self.allow_column_projection_passthrough:
            return self, projection
        else:
            return self, None
