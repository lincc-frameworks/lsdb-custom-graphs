from pathlib import Path

import lsdb
from pytest import fixture


@fixture
def hats_dir():
    return Path("/mnt/data/hats/catalogs/")


@fixture
def gaia_dir(hats_dir):
    return hats_dir / "gaia_dr3"


@fixture
def ztf_dir(hats_dir):
    return hats_dir / "ztf_dr22"


@fixture
def des_dir(hats_dir):
    return hats_dir / "des/des_dr2"


@fixture
def gaia(gaia_dir):
    return lsdb.open_catalog(gaia_dir)


@fixture
def ztf(ztf_dir):
    return lsdb.open_catalog(ztf_dir)


@fixture
def des(des_dir):
    return lsdb.open_catalog(des_dir)
