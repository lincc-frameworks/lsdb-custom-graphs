from dask._task_spec import Task
from dask.tokenize import _tokenize_deterministic
from dask.utils import funcname

import pandas as pd
from hats import HealpixPixel

from lsdb_custom_graphs.lsdb.ops.operation import HealpixGraph, Operation


class FromHealpixMap(Operation):
    def __init__(self, func, pixels, *args, meta=None, **kwargs):
        self.func = func
        self.pixels = pixels
        self.args = args
        self._meta = meta
        self.kwargs = kwargs

    @property
    def meta(self) -> pd.DataFrame:
        if self._meta is not None:
            return self._meta
        else:
            first_part = self.func(self.pixels[0], *self.args, **self.kwargs)
            if not isinstance(first_part, pd.DataFrame):
                raise ValueError("FromMap function must return a pandas DataFrame")
            return first_part.iloc[:0].copy()

    def build(self) -> HealpixGraph:
        graph = {}
        pixel_keys = {}
        for i, pixel in enumerate(self.pixels):
            key_name = f"{funcname(self.func)}-{_tokenize_deterministic(pixel, self.args, self.kwargs)}"
            key = (key_name, i)
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
    def __init__(self, base: Operation, func, *args, meta=None, include_pixel=False, **kwargs):
        self.base = base
        self.func = func
        self.args = args
        self._meta = meta
        self.include_pixel = include_pixel
        self.kwargs = kwargs

    @property
    def meta(self) -> pd.DataFrame:
        if self._meta is not None:
            return self._meta
        else:
            return map_parts_meta(self.func, self.base.meta(), *self.args, include_pixel=self.include_pixel,
                                  **self.kwargs)

    def build(self) -> HealpixGraph:
        previous = self.base.build()
        graph = previous.graph
        pixel_keys = {}
        for i, (pixel, prev_key) in enumerate(previous.pixel_to_key_map.items()):
            args = self.args
            if self.include_pixel:
                args = (HealpixPixel(*pixel),) + args
            key_name = f"{funcname(self.func)}-{_tokenize_deterministic(prev_key, args, self.kwargs)}"
            key = (key_name, i)
            task = Task(key, self.func, prev_key, *args, **self.kwargs)
            graph[key] = task
            pixel_keys[pixel] = key
        return HealpixGraph(graph, pixel_keys)
