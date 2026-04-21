from __future__ import annotations

import functools
import inspect
from typing import TYPE_CHECKING, Self

from dask._task_spec import Task, TaskRef, cull
from dask.tokenize import _tokenize_deterministic
from dask.utils import funcname

import pandas as pd
from hats import HealpixPixel
from pyarrow.lib import Sequence

from lsdb_custom_graphs.lsdb.ops.operation import HealpixGraph, Operation, projection_handler, \
    projection_emitter
from lsdb_custom_graphs.lsdb.ops.projections import ColumnProjection

if TYPE_CHECKING:
    from lsdb_custom_graphs.lsdb.healpix_dataset import HealpixDataset


def supports_column_selection(func):
    """Check if a function has a `columns` argument that can be used to specify the output columns."""
    return "columns" in inspect.signature(func).parameters


class FromHealpixMap(Operation):
    def __init__(self, func, pixels, *args, meta=None, columns=None, **kwargs):
        self.func = func
        self.pixels = pixels
        self.args = args
        self._meta = meta
        if columns is not None:
            kwargs = kwargs | {"columns": columns}
        self.kwargs = kwargs

    @projection_handler(ColumnProjection)
    def handle_column_projection(self, projection: ColumnProjection) -> FromHealpixMap:
        if not supports_column_selection(self.func):
            return self
        return FromHealpixMap(self.func, self.pixels, *self.args, meta=self.meta,
                              columns=projection.column_selector, **self.kwargs)

    @property
    def name(self) -> str:
        return f"FromHealpixMap({funcname(self.func)})"

    @functools.cached_property
    def key_name(self) -> str:
        return f"{funcname(self.func)}-{_tokenize_deterministic(*self.args, self.kwargs)}"

    @property
    def meta(self) -> pd.DataFrame:
        if self._meta is not None:
            return self._meta
        else:
            first_part = self.func(self.pixels[0], *self.args, **self.kwargs)
            if not isinstance(first_part, pd.DataFrame):
                raise ValueError("FromMap function must return a pandas DataFrame")
            return first_part.iloc[:0].copy()

    @property
    def dependencies(self) -> list[Self]:
        return []

    def replace_dependencies(self, dependencies: list[Self]) -> Self:
        if len(dependencies) > 0:
            raise ValueError("FromHealpixMap cannot have dependencies")
        return self

    def build(self) -> HealpixGraph:
        graph = {}
        pixel_keys = {}
        for i, pixel in enumerate(self.pixels):
            key = (self.key_name, i)
            task = Task(key, self.func, pixel, *self.args, **self.kwargs)
            graph[key] = task
            pixel_keys[pixel] = key
        return HealpixGraph(graph, pixel_keys)


def map_parts_meta(func, base_meta: pd.DataFrame, *args, include_pixel=False, **kwargs) -> pd.DataFrame:
    try:
        if include_pixel:
            return func(base_meta, HealpixPixel(0, 0), *args, **kwargs)
        return func(base_meta, *args, **kwargs)
    except Exception as e:
        raise ValueError("Cannot infer meta for MapPartitions. Either make sure your function works with an"
                         " empty dataframe input, or supply a meta for your function") from e


class MapPartitions(Operation):
    class_func = None
    class_include_pixels = None

    def __init__(self, base: Operation, func, *args, meta=None, include_pixel=False, **kwargs):
        self.base = base
        if self.class_func is not None and func is not None:
            raise ValueError("Cannot specify func for MapPartitions when class_func is set")
        if self.class_include_pixels is not None and include_pixel != self.class_include_pixels:
            raise ValueError(
                "Cannot specify include_pixel for MapPartitions when class_include_pixels is set")
        if func is None and self.class_func is not None:
            func = self.class_func
        self.func = func
        self.args = args
        self._meta = meta
        self.include_pixel = include_pixel
        self.kwargs = kwargs

    @property
    def name(self) -> str:
        return f"MapPartitions({funcname(self.func)})"

    @functools.cached_property
    def key_name(self) -> str:
        return f"{funcname(self.func)}-{_tokenize_deterministic(self.base.meta, self.base.name, self.args, self.kwargs)}"

    @property
    def meta(self) -> pd.DataFrame:
        if self._meta is not None:
            return self._meta
        else:
            return map_parts_meta(self.func, self.base.meta, *self.args, include_pixel=self.include_pixel,
                                  **self.kwargs)

    @property
    def dependencies(self) -> list[Operation]:
        return [self.base]

    def replace_dependencies(self, dependencies: list[Operation]) -> Self:
        if len(dependencies) != 1:
            raise ValueError("MapPartitions must have exactly one dependency")
        return MapPartitions(dependencies[0], self.func, *self.args, meta=self._meta,
                             include_pixel=self.include_pixel, **self.kwargs)

    def build(self) -> HealpixGraph:
        previous = self.base.build()
        graph = previous.graph
        pixel_keys = {}
        for i, (pixel, prev_key) in enumerate(previous.pixel_to_key_map.items()):
            args = self.args
            if self.include_pixel:
                args = (HealpixPixel(*pixel),) + args
            key = (self.key_name, i)
            task = Task(key, self.func, TaskRef(prev_key), *args, **self.kwargs)
            graph[key] = task
            pixel_keys[pixel] = key
        return HealpixGraph(graph, pixel_keys)


def perform_select_columns(df, columns):
    return df[columns]


class SelectColumns(MapPartitions):
    allow_column_projection_passthrough = True

    @staticmethod
    def class_func(df, item):
        return df[item]

    @property
    def column_selector(self):
        return self.args[0]

    @projection_emitter(ColumnProjection)
    def emit_column_projection(self) -> ColumnProjection:
        return ColumnProjection(self.column_selector)


class SelectPixels(Operation):
    allow_column_projection_passthrough = True

    def __init__(self, base: Operation, pixels: Sequence[HealpixPixel]):
        self.base = base
        self.pixels = pixels

    @property
    def name(self) -> str:
        return f"SelectPixels"

    @functools.cached_property
    def key_name(self) -> str:
        return f"select_pixels-{_tokenize_deterministic(self.base.meta, self.base.name, *self.pixels)}"

    @property
    def meta(self) -> pd.DataFrame:
        return self.base.meta

    @property
    def dependencies(self) -> list[Self]:
        return [self.base]

    def replace_dependencies(self, dependencies: list[Self]) -> Self:
        if len(dependencies) != 1:
            raise ValueError("SelectPixels must have exactly one dependency")
        return SelectPixels(dependencies[0], self.pixels)

    def build(self) -> HealpixGraph:
        previous = self.base.build()
        selected_pixels = self.pixels
        for p in selected_pixels:
            if p not in previous.pixel_to_key_map:
                raise ValueError(f"Selected Pixel {p} not found in operation")
        selected_keys = [previous.pixel_to_key_map[p] for p in selected_pixels]
        culled_graph = cull(previous.graph, selected_keys)
        pixel_keys = {p: k for p, k in zip(selected_pixels, selected_keys)}
        return HealpixGraph(culled_graph, pixel_keys)


class AlignAndApply(Operation):
    def __init__(self, input_cats: Sequence[HealpixDataset],
                 pixel_lists: Sequence[Sequence[HealpixPixel | None]],
                 func,
                 meta, output_pixels: Sequence[HealpixPixel], *args, **kwargs):
        self.input_cats = input_cats
        self.pixel_lists = pixel_lists
        if len(self.input_cats) != len(self.pixel_lists):
            raise ValueError("Inccorect Align and Apply Setup")
        self.func = func
        self._meta = meta
        self.output_pixels = output_pixels
        self.args = args
        self.kwargs = kwargs

    @property
    def input_ops(self):
        return [cat.operation if cat is not None else None for cat in self.input_cats]

    @property
    def dependencies(self) -> list[Self]:
        return [op for op in self.input_ops if op is not None]

    def replace_dependencies(self, dependencies: list[Self]) -> Self:
        deps = dependencies.copy()
        non_none_ops = [op for op in self.input_ops if op is not None]
        if len(dependencies) != len(non_none_ops):
            raise ValueError("AlignAndApply must have exactly one dependency for each non-None input cat")
        new_input_cats = []
        for ic in self.input_cats:
            if ic is not None:
                new_op = deps.pop(0)
                new_input_cats.append(ic._create_updated_dataset(op=new_op))
            else:
                new_input_cats.append(None)
        return AlignAndApply(
            new_input_cats, self.pixel_lists, self.func, self._meta, self.output_pixels, self.args,
            self.kwargs
        )

    @property
    def metas(self):
        return [op.meta if op is not None else None for op in self.input_ops]

    @property
    def catalog_infos(self):
        return [cat.hc_structure.catalog_info if cat is not None else None for cat in
                self.input_cats]

    @property
    def name(self) -> str:
        return f"AlignAndApply({funcname(self.func)})"

    @functools.cached_property
    def key_name(self) -> str:
        names = [op.name if op is not None else None for op in self.input_ops]
        return f"{funcname(self.func)}-{_tokenize_deterministic(*self.metas, *names, *self.pixel_lists, *self.catalog_infos, *self.args, self.kwargs)}"

    @property
    def meta(self) -> pd.DataFrame:
        return self._meta

    def build(self) -> HealpixGraph:
        input_ops = self.input_ops
        graphs = [op.build() if op is not None else None for op in input_ops]
        metas = self.metas
        catalog_infos = self.catalog_infos
        graph = {}
        pixel_key_map = {}
        for g in graphs:
            if g is not None:
                graph = graph | g.graph
        for i, all_pixels in enumerate(zip(self.output_pixels, *self.pixel_lists)):
            output_pixel = all_pixels[0]
            pixels = all_pixels[1:]
            task_refs = []
            for g, m, p in zip(graphs, metas, pixels):
                if g is None:
                    task_refs.append(None)
                elif p is None or p not in g.pixel_to_key_map:
                    task_refs.append(m)
                else:
                    task_refs.append(TaskRef(g.pixel_to_key_map[p]))
            args = task_refs + list(pixels) + catalog_infos + list(self.args)
            kwargs = self.kwargs
            key = (self.key_name, i)
            task = Task(key, self.func, *args, **kwargs)
            graph[key] = task
            pixel_key_map[output_pixel] = key
        culled_graph = cull(graph, list(pixel_key_map.values()))
        return HealpixGraph(culled_graph, pixel_key_map)
