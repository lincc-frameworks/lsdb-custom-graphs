from typing import Callable, Sequence, Self

from dask import threaded
from dask.base import get_scheduler
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset as HCHealpixDataset
import numpy as np
import pandas as pd
import nested_pandas as npd
import dask.dataframe as dd
from dask.dataframe.core import _repr_data_series
from dask.optimization import cull
from dask.delayed import Delayed
from hats import HealpixPixel
from hats.pixel_math.healpix_pixel_function import get_pixel_argsort

import pyarrow as pa
from lsdb.core.search.abstract_search import AbstractSearch

from lsdb_custom_graphs.lsdb.ops.lsdb_ops import MapPartitions, SelectPixels
from lsdb_custom_graphs.lsdb.ops.operation import Operation
from lsdb_custom_graphs.lsdb.partition_indexer import PartitionIndexer


def get_arrow_schema(df) -> pa.Schema:
    """Constructs the pyarrow schema from the meta of a Dask DataFrame.

    Parameters
    ----------
    ddf : dd.DataFrame
        A Dask DataFrame.

    Returns
    -------
    pa.Schema
        The arrow schema for the provided Dask DataFrame.
    """
    # pylint: disable=protected-access
    return pa.Schema.from_pandas(df).remove_metadata()


class HealpixDataset:
    def __init__(self, operation: Operation, hc_structure: HCHealpixDataset):
        self.operation = operation
        self.hc_structure = hc_structure

    @property
    def name(self):
        """The name of the catalog"""
        return self.hc_structure.catalog_name

    @property
    def columns(self):
        return self.operation.meta.columns

    @property
    def all_columns(self):
        """Returns the names of all columns in the original Dataset.

        This is different from the `columns` property, as you can open a catalog with
        only a subset of the columns, either explicitly with the ``columns=`` argument,
        or with some ``default_columns`` set on the catalog by the catalog provider."""
        if self.hc_structure.original_schema is None:
            # This case corresponds to Datasets that have not yet been
            # serialized, and thus cannot have a discrepancy between
            # the original schema and the loaded schema.  In this case,
            # this property is equivalent to the columns property.
            return self.columns
        col_names = self.hc_structure.original_schema.names
        if self.operation.meta.index.name in col_names:
            col_names.remove(self.operation.meta.index.name)
        return col_names

    def get_ordered_healpix_pixels(self) -> Sequence[HealpixPixel]:
        """Get all HEALPix pixels that are contained in the catalog,
        ordered by breadth-first nested ordering.

        Returns
        -------
        list[HealpixPixel]
            List of all Healpix pixels in the catalog
        """
        pixels = self.get_healpix_pixels()
        return np.array(pixels)[get_pixel_argsort(pixels)]

    def get_healpix_pixels(self):
        return self.hc_structure.get_healpix_pixels()

    def __repr__(self):
        return f"LSDB expr Catalog(operation={self.operation}) Schema:\n" + self.operation.meta.__repr__()

    @property
    def _repr_divisions(self):
        pixels = self.get_healpix_pixels()
        name = f"npartitions={len(pixels)}"
        # Dask will raise an exception, preventing display,
        # if the index does not have at least one element.
        if len(pixels) == 0:
            pixels = ["Empty Catalog"]
        divisions = pd.Index(pixels, name=name)
        return divisions

    def _repr_data(self):
        meta = self.operation.meta
        index = self._repr_divisions
        cols = meta.columns
        if len(cols) == 0:
            series_df = pd.DataFrame([[]] * len(index), columns=cols, index=index)
        else:
            series_df = pd.concat([_repr_data_series(s, index=index) for _, s in meta.items()], axis=1)
        return series_df

    def _repr_html_(self):
        data = self._repr_data().to_html(max_rows=5, show_dimensions=False, notebook=True)
        loaded_cols = len(self.columns)
        available_cols = len(self.all_columns)
        return (
            f"<div><strong>lsdb Catalog {self.name}:</strong></div>"
            f"{data}"
            f"<div>{loaded_cols} out of {available_cols} available columns in the catalog have been loaded "
            f"<strong>lazily</strong>, meaning no data has been read, only the catalog schema</div>"
        )

    def _create_modified_hc_structure(
            self, hc_structure=None, updated_schema=None, **kwargs
    ) -> HCHealpixDataset:
        """Copy the catalog structure and override the specified catalog info parameters."""
        if hc_structure is None:
            hc_structure = self.hc_structure
        return hc_structure.__class__(
            catalog_info=hc_structure.catalog_info.copy_and_update(**kwargs),
            pixels=hc_structure.pixel_tree,
            catalog_path=hc_structure.catalog_path,
            schema=hc_structure.schema if updated_schema is None else updated_schema,
            snapshot=hc_structure.snapshot,
            moc=hc_structure.moc,
        )

    def _create_updated_dataset(
            self,
            op: Operation | None = None,
            hc_structure: HCHealpixDataset | None = None,
            updated_catalog_info_params: dict | None = None,
    ) -> Self:
        """Creates a new copy of the catalog, updating any provided arguments

        Shallow copies the ddf and ddf_pixel_map if not provided. Creates a new hc_structure if not provided.
        Updates the hc_structure with any provided catalog info parameters, resets the total rows, removes
        any default columns that don't exist, and updates the pyarrow schema to reflect the new ddf.

        Parameters
        ----------
        ddf : nd.NestedFrame or None, default None
            The catalog ddf to update in the new catalog
        ddf_pixel_map : DaskDFPixelMap or None, default None
            The partition to healpix pixel map to update in the new catalog
        hc_structure : HCHealpixDataset or None, default None
            The hats HealpixDataset object to update in the new catalog
        updated_catalog_info_params : dict or None, default None
            The dictionary of updates to the parameters of the hats dataset object's catalog_info

        Returns
        -------
        Self
            A new dataset object with the arguments updated to those provided to the function, and the
            hc_structure metadata updated to match the new ddf
        """
        op = op if op is not None else self.operation
        hc_structure = hc_structure if hc_structure is not None else self.hc_structure
        updated_catalog_info_params = updated_catalog_info_params or {}
        if (
                "default_columns" not in updated_catalog_info_params
                and hc_structure.catalog_info.default_columns is not None
        ):
            updated_catalog_info_params["default_columns"] = [
                col for col in hc_structure.catalog_info.default_columns if col in op.meta.columns
            ]
        if "total_rows" not in updated_catalog_info_params:
            updated_catalog_info_params["total_rows"] = None
        updated_schema = get_arrow_schema(op.meta)
        hc_structure = self._create_modified_hc_structure(
            hc_structure=hc_structure, updated_schema=updated_schema, **updated_catalog_info_params
        )
        return self.__class__(op, hc_structure)

    def map_partitions(self, func, *args, meta=None, include_pixel=False, **kwargs):
        new_op = MapPartitions(self.operation, func, *args, meta=meta, include_pixel=include_pixel, **kwargs)
        return self._create_updated_dataset(op=new_op)

    def __getitem__(self, item: str | list[str]) -> Self:
        """Select a column or columns from the catalog, always returning a catalog (not a Series)."""
        columns = [item] if isinstance(item, str) else list(item)
        new_op = MapPartitions(self.operation, pd.DataFrame.__getitem__, columns)
        return self._create_updated_dataset(op=new_op)

    def query(self, expr: str) -> Self:
        """Filters catalog using a pandas query expression.

        Parameters
        ----------
        expr : str
            Query expression to evaluate. The column names that are not valid Python
            variables names should be wrapped in backticks, and any variable values can be
            injected using f-strings. The use of '@' to reference variables is not supported.
            More information about pandas query strings is available
            `here <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.query.html>`__.

        Returns
        -------
        Self
            A catalog that contains the data from the original catalog that complies
            with the query expression.
        """
        return self.map_partitions(npd.NestedFrame.query, expr)

    def drop(self, columns: str | list[str], errors: str = "raise") -> Self:
        """Drop specified columns from the catalog.

        Parameters
        ----------
        columns : single label or list-like
            Column labels to drop.
        errors : {'ignore', 'raise'}, default 'raise'
            If 'ignore', suppress error and only existing labels are dropped.

        Returns
        -------
        Self
            A catalog containing all columns except for those specified.
        """
        return self.map_partitions(npd.NestedFrame.drop, columns=columns, errors=errors)

    def rename(self, columns: dict | Callable) -> Self:
        """Renames catalog columns (not indices) using a dictionary or function mapping.

        Parameters
        ----------
        columns : dict-like or function
            Transformations to apply to column names.

        Returns
        -------
        Self
            A catalog that contains the data from the original catalog with renamed columns.
        """
        updated_params = {}
        meta = self.operation.meta
        ra_col = self.hc_structure.catalog_info.ra_column
        dec_col = self.hc_structure.catalog_info.dec_column
        if ra_col in meta.columns:
            new_ra = meta[[ra_col]].rename(columns=columns).columns[0]
            if new_ra != ra_col:
                updated_params["ra_column"] = new_ra
        if dec_col in meta.columns:
            new_dec = meta[[dec_col]].rename(columns=columns).columns[0]
            if new_dec != dec_col:
                updated_params["dec_column"] = new_dec
        new_cat = self.map_partitions(npd.NestedFrame.rename, columns=columns)
        return new_cat._create_updated_dataset(updated_catalog_info_params=updated_params)

    def search(self, search: AbstractSearch):
        filtered_cat = search.filter_hc_catalog(self.hc_structure)
        filtered_pixels = filtered_cat.get_healpix_pixels()
        filtered_op = SelectPixels(self.operation, filtered_pixels)
        if search.fine:
            filtered_op = MapPartitions(filtered_op, search.search_points, filtered_cat.catalog_info)
        return self._create_updated_dataset(filtered_op, hc_structure=filtered_cat)

    @property
    def partitions(self):
        return PartitionIndexer(self)

    def compute(self):
        schedule = get_scheduler()
        if schedule is None:
            schedule = threaded.get
        healpix_graph = self.operation.build()
        result = schedule(healpix_graph.graph, healpix_graph.keys)
        return pd.concat(result)

    def to_dask(self, optimize_graph=False):
        """Converts to a lsdb.nested Dask DataFrame

        Parameters
        ----------
        optimize_graph : bool, default False
            Whether to perform graph optimization before creating the
            Dask DataFrame. By default False, as it should not be necessary in
            most cases.
        """
        build = self.operation.build()
        graph = build.graph
        meta = self.operation.meta
        keys = build.keys

        graph_delayed = [Delayed(key, graph) for key in keys]

        if not optimize_graph:
            return dd.from_delayed(graph_delayed, meta=meta)

        # I don't know if we will actually ever need to do this
        shared_graph = dict(graph_delayed[0].__dask_graph__())
        optimized_graph = []
        for d in graph_delayed:
            culled_graph, _ = cull(shared_graph, list(d.__dask_keys__()))
            optimized_graph.append(Delayed(d.key, culled_graph))
        return dd.from_delayed(optimized_graph, meta=meta)
