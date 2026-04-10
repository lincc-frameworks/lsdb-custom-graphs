"""Top-level API for visualising Dask task graphs."""

from .graph_converter import dask_graph_to_networkx
from .layout import compute_layout
from .renderer import render_graph


def visualise_graph(dask_graph: dict, width: int = 1000, height: int = 600, calculate_memory: bool = True):
    """Visualise a Dask task graph in a Jupyter notebook.

    Parameters
    ----------
    dask_graph : dict
        A Dask task graph, e.g. from ``ddf.__dask_graph__()`` or ``ddf.dask``.
    width : int
        Plot width in pixels.
    height : int
        Plot height in pixels.

    Returns
    -------
    panel.pane.Bokeh
        The Panel pane wrapping the Bokeh figure.
    """
    G = dask_graph_to_networkx(dask_graph)
    positions = compute_layout(G)
    return render_graph(G, positions, dask_graph, width=width, height=height, calculate_memory=calculate_memory)
