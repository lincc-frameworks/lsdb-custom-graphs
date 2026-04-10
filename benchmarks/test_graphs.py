import lsdb
import pytest


@pytest.mark.lbench_memory
def test_gaia_graph_construction(gaia, lbench):
    def build_graph():
        g = gaia._ddf.dask

    lbench(build_graph)


@pytest.mark.lbench_memory
def test_gaia_ztf_xmatch_graph_construction(gaia, ztf, lbench):
    xmatch = gaia.crossmatch(ztf)

    def build_graph():
        g = xmatch._ddf.dask

    lbench(build_graph)


@pytest.mark.lbench_memory
def test_gaia_ztf_xmatch_lazy(gaia, ztf, lbench):
    def lazy_xmatch():
        return gaia.crossmatch(ztf)

    lbench(lazy_xmatch)


@pytest.mark.benchmark(min_rounds=1)
@pytest.mark.parametrize("n_partitions", [1])
def test_gaia_ztf_xmatch(gaia, ztf, lbench_dask_collection, n_partitions):
    xmatch = gaia.crossmatch(ztf).partitions[:n_partitions].map_partitions(
        lambda df: df.head(1))
    lbench_dask_collection(xmatch._ddf)


@pytest.mark.parametrize("n_partitions", [1, 10])
def test_gaia_ztf_xmatch_cols(gaia_dir, ztf_dir, lbench_dask_collection, n_partitions):
    gaia = lsdb.open_catalog(gaia_dir, columns=["ra", "dec"])
    ztf = lsdb.open_catalog(ztf_dir, columns=["objra", "objdec"])
    xmatch = gaia.crossmatch(ztf).partitions[:n_partitions].map_partitions(
        lambda df: df.head(1))
    lbench_dask_collection(xmatch._ddf)
