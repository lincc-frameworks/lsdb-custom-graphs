import warnings
from typing import Self

import hats
from hats.catalog.healpix_dataset.healpix_dataset import HealpixDataset as HCHealpixDataset
from lsdb.catalog.catalog import _default_suffixes
from lsdb.core.crossmatch.abstract_crossmatch_algorithm import AbstractCrossmatchAlgorithm
from lsdb.core.crossmatch.kdtree_match import KdTreeCrossmatch
from lsdb.core.search.abstract_search import AbstractSearch
from lsdb.dask.merge_catalog_functions import create_merged_catalog_info, DEFAULT_SUFFIX_METHOD

from lsdb_custom_graphs.lsdb import HealpixDataset
from lsdb_custom_graphs.lsdb.crossmatch_catalog_data import crossmatch_catalog_data
from lsdb_custom_graphs.lsdb.healpix_dataset import get_arrow_schema
from lsdb_custom_graphs.lsdb.margin_catalog import MarginCatalog
from lsdb_custom_graphs.lsdb.ops.operation import Operation


class Catalog(HealpixDataset):

    def __init__(self, operation: Operation, hc_structure: hats.catalog.Catalog,
                 margin: MarginCatalog = None):
        super().__init__(operation, hc_structure)
        self.margin = margin

    def map_partitions(self, func, *args, meta=None, include_pixel=False, **kwargs):
        cat = super().map_partitions(func, *args, meta=meta, include_pixel=include_pixel, **kwargs)
        margin = self.margin.map_partitions(func, *args, meta=meta, include_pixel=include_pixel,
                                            **kwargs) if self.margin is not None else None
        cat.margin = margin
        return cat

    def search(self, search: AbstractSearch):
        cat = super().search(search)
        margin = self.margin.search(search) if self.margin is not None else None
        cat.margin = margin
        return cat

    def _create_updated_dataset(
            self,
            op: Operation | None = None,
            hc_structure: HCHealpixDataset | None = None,
            updated_catalog_info_params: dict | None = None,
            margin: MarginCatalog | None = None,
    ) -> Self:
        new_dataset = super()._create_updated_dataset(op=op, hc_structure=hc_structure,
                                                      updated_catalog_info_params=updated_catalog_info_params)
        if margin is None and op is None and hc_structure is None and updated_catalog_info_params:
            margin = self.margin._create_updated_datset(
                updated_catalog_info_params=updated_catalog_info_params) if self.margin is not None else None
        new_dataset.margin = margin
        return new_dataset

    def crossmatch(
            self,
            other: Self,
            *,
            n_neighbors: int | None = None,
            radius_arcsec: float | None = None,
            min_radius_arcsec: float | None = None,
            algorithm: AbstractCrossmatchAlgorithm | None = None,
            output_catalog_name: str | None = None,
            require_right_margin: bool = False,
            how: str = "inner",
            suffixes: tuple[str, str] | None = None,
            suffix_method: str | None = None,
            log_changes: bool = True,
    ) -> Self:
        # pylint:disable=unused-argument
        """Perform a cross-match between two catalogs

        The pixels from each catalog are aligned via a `PixelAlignment`, and cross-matching is
        performed on each pair of overlapping pixels. The resulting catalog will have partitions
        matching an inner pixel alignment - using pixels that have overlap in both input catalogs
        and taking the smallest of any overlapping pixels.

        The resulting catalog will be partitioned using the left catalog's ra and dec, and the
        index for each row will be the same as the index from the corresponding row in the left
        catalog's index.

        Parameters
        ----------
        other : Catalog
            The right catalog to cross-match against
        n_neighbors : int, default 1
            The number of neighbors to find within each point.
        radius_arcsec : float, default 1.0
            The threshold distance in arcseconds beyond which neighbors are not added.
        min_radius_arcsec : float, default 0.0
            The threshold distance in arcseconds beyond which neighbors are added.
        algorithm : AbstractCrossmatchAlgorithm | None, default `KDTreeCrossmatch`
            The instance of an algorithm used to perform the crossmatch. If None,
            the default KDTree crossmatch algorithm is used. If specified, the
            algorithm is defined by subclassing `AbstractCrossmatchAlgorithm`.

            Default algorithm:
                - `KdTreeCrossmatch`: find the k-nearest neighbors using a kd_tree

            Custom algorithm:
                To specify a custom algorithm, write a class that subclasses the
                `AbstractCrossmatchAlgorithm` class, and either overwrite the `crossmatch`
                or the `perform_crossmatch` function.

                The function should be able to perform a crossmatch on two pandas DataFrames
                from a partition from each catalog. It should return two 1d numpy arrays of equal lengths
                with the indices of the matching rows from the left and right dataframes, and a dataframe
                with any extra columns generated by the crossmatch algorithm, also with the same length.
                These columns are specified in {AbstractCrossmatchAlgorithm.extra_columns}, with
                their respective data types, by means of an empty pandas dataframe. As an example,
                the KdTreeCrossmatch algorithm outputs a "_dist_arcsec" column with the distance between
                data points. Its extra_columns attribute is specified as follows::

                    pd.DataFrame({"_dist_arcsec": pd.Series(dtype=np.dtype("float64"))})

                The `crossmatch`/`perform_crossmatch` methods will receive an instance of `CrossmatchArgs`
                which includes the partitions and respective pixel information::

                    - left_df: npd.NestedFrame
                    - right_df: npd.NestedFrame
                    - left_order: int
                    - left_pixel: int
                    - right_order: int
                    - right_pixel: int
                    - left_catalog_info: hc.catalog.TableProperties
                    - right_catalog_info: hc.catalog.TableProperties
                    - right_margin_catalog_info: hc.catalog.TableProperties

                Include any algorithm-specific parameters in the initialization of your object.
                These parameters should be validated in `AbstractCrossmatchAlgorithm.validate`,
                by overwriting the method.

        output_catalog_name : str, default {left_name}_x_{right_name}
            The name of the resulting catalog.
        require_right_margin : bool, default False
            If true, raises an error if the right margin is missing which could
            lead to incomplete crossmatches.
        how : str
            How to handle the crossmatch of the two catalogs.
            One of {'left', 'inner'}; defaults to 'inner'.
        suffixes : Tuple[str,str] or None
            A pair of suffixes to be appended to the end of each column
            name when they are joined. Default uses the name of the catalog for the suffix.
        suffix_method : str or None, default "all_columns"
            Method to use to add suffixes to columns. Options are:

            - "overlapping_columns": only add suffixes to columns that are present in both catalogs
            - "all_columns": add suffixes to all columns from both catalogs

            .. warning:: This default will change to "overlapping_columns" in a future release.

        log_changes : bool, default True
            If True, logs an info message for each column that is being renamed.
            This only applies when suffix_method is 'overlapping_columns'.

        Returns
        -------
        Catalog
            A Catalog with the data from the left and right catalogs merged with one row for each
            pair of neighbors found from cross-matching.
            The resulting table contains all columns from the left and right catalogs with their
            respective suffixes and, whenever specified, a set of extra columns generated by the
            crossmatch algorithm.

        Examples
        --------
        Crossmatch two small synthetic catalogs:

        >>> import lsdb
        >>> from lsdb.nested.datasets import generate_data
        >>> nf = generate_data(1000, 5, seed=0, ra_range=(0.0, 300.0), dec_range=(-50.0, 50.0))
        >>> df = nf.compute()[["ra", "dec", "id"]]
        >>> left = lsdb.from_dataframe(df, catalog_name="left")
        >>> right = lsdb.from_dataframe(df, catalog_name="right")
        >>> xmatch = left.crossmatch(right, n_neighbors=1, radius_arcsec=1.0,
        ... suffix_method="overlapping_columns", log_changes=False)
        >>> xmatch.head()[  # doctest: +NORMALIZE_WHITESPACE
        ...     ["ra_left", "dec_left", "id_left", "_dist_arcsec"]
        ... ]
                    ra_left   dec_left  id_left  _dist_arcsec
        _healpix_29
        118362963675428450  52.696686  39.675892    8154           0.0
        98504457942331510   89.913567  46.147079    3437           0.0
        70433374600953220   40.528952  35.350965    8214           0.0
        154968715224527848   17.57041    29.8936    9853           0.0
        67780378363846894    45.08384   31.95611    8297           0.0

        Raises
        ------
        TypeError
            If the `other` catalog is not of type `Catalog`
        ValueError
            If both the kwargs for the default algorithm and an `algorithm` are specified.
            If the `suffixes` provided is not a tuple of two strings.
            If the right catalog has no margin and `require_right_margin` is True.
        """
        if not isinstance(other, Catalog):
            raise TypeError(
                f"Expected `other` to be a Catalog instance, got {type(other)}. "
                "You may want `lsdb.crossmatch(frame_or_catalog, frame_or_catalog)` instead."
            )

        default_kwargs = {
            k: v
            for k, v in locals().items()
            if k in ("radius_arcsec", "n_neighbors", "min_radius_arcsec") and v is not None
        }
        if not algorithm:
            algorithm = KdTreeCrossmatch(**default_kwargs)
        elif any(default_kwargs.values()):
            raise ValueError(f"If you specify `algorithm`, do not set {list(default_kwargs.keys())}")

        if suffixes is None:
            suffixes = _default_suffixes(self.name, other.name)
        if len(suffixes) != 2:
            raise ValueError("`suffixes` must be a tuple with two strings")
        if suffix_method is None:
            suffix_method = DEFAULT_SUFFIX_METHOD
            warnings.warn(
                "The default suffix behavior will change from applying suffixes to all columns to only "
                "applying suffixes to overlapping columns in a future release."
                "To maintain the current behavior, explicitly set `suffix_method='all_columns'`. "
                "To change to the new behavior, set `suffix_method='overlapping_columns'`.",
                FutureWarning,
            )
        if other.margin is None and require_right_margin:
            raise ValueError("Right catalog margin cache is required for cross-match.")
        if output_catalog_name is None:
            output_catalog_name = f"{self.name}_x_{other.name}"

        new_op, alignment = crossmatch_catalog_data(
            self,
            other,
            algorithm,
            how,
            suffixes,
            suffix_method,
            log_changes,
        )
        new_catalog_info = create_merged_catalog_info(
            self,
            other,
            output_catalog_name,
            suffixes,
            suffix_method,
        )
        hc_catalog = self.hc_structure.__class__(
            new_catalog_info, alignment.pixel_tree, schema=get_arrow_schema(new_op.meta), moc=alignment.moc
        )
        return self.__class__(new_op, hc_catalog)
