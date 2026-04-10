from lsdb_custom_graphs import lsdb
import pytest


@pytest.mark.lbench_memory
def test_gaia_graph_construction(gaia, lbench):
    def build_graph():
        g = gaia.operation.build()

    lbench(build_graph)


@pytest.mark.lbench_memory
def test_gaia_ztf_xmatch_graph_construction(gaia, ztf, lbench):
    xmatch = gaia.crossmatch(ztf)

    def build_graph():
        g = xmatch.operation.build()

    lbench(build_graph)


@pytest.mark.lbench_memory
def test_gaia_ztf_xmatch_lazy(gaia, ztf, lbench):
    def lazy_xmatch():
        return gaia.crossmatch(ztf)

    lbench(lazy_xmatch)


# @pytest.mark.benchmark(min_rounds=1)
# @pytest.mark.parametrize("n_partitions", [1])
# def test_gaia_des_xmatch(gaia, des, lbench_dask_collection, n_partitions):
#     xmatch = gaia.crossmatch(des).partitions[:n_partitions].map_partitions(
#         lambda df: df.head(1))
#     lbench_dask_collection(xmatch._ddf, measure_memory=False)


@pytest.mark.parametrize("n_partitions", [1])
def test_gaia_ztf_xmatch_cols(gaia_dir, des_dir, lbench_dask, n_partitions):
    gaia = lsdb.open_catalog(gaia_dir, columns=["ra", "dec"])
    des = lsdb.open_catalog(des_dir, columns=["RA", "DEC"])
    xmatch = gaia.crossmatch(des).partitions[:n_partitions].map_partitions(
        lambda df: df.head(1))
    lbench_dask(lambda: xmatch.compute(), measure_memory=False)
