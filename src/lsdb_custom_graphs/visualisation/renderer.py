"""Bokeh renderer for Dask task graphs with viewport-based JS filtering."""

import math
from pathlib import Path

import networkx as nx
from bokeh.io import output_notebook, show
from bokeh.layouts import column, row
from bokeh.models import (
    Button,
    ColorBar,
    ColumnDataSource,
    CustomJS,
    HoverTool,
    LinearColorMapper,
    MultiLine,
    Range1d,
    Rect,
    Scatter,
    Text,
    WheelZoomTool,
)
from bokeh.palettes import Category20, Viridis256
from bokeh.plotting import figure

from .memory import estimate_task_memory, format_bytes


def render_graph(
    G: nx.DiGraph,
    positions: dict,
    dask_graph: dict,
    width: int = 1000,
    height: int = 600,
    calculate_memory: bool = True,
):
    """Render a Dask task graph with client-side viewport filtering.

    All node/edge data is sent to the browser once. A JS callback filters
    the visible data sources on pan/zoom so only visible elements are rendered.

    Scroll to zoom vertically (through partitions). Shift+scroll to zoom both axes.

    Parameters
    ----------
    G : nx.DiGraph
        Graph with 'task_name' and 'partition' node attributes.
    positions : dict
        Mapping of node key to (x, y) position.
    dask_graph : dict
        The original Dask task graph dict, used for tooltip repr.
    width : int
        Plot width in pixels.
    height : int
        Plot height in pixels.
    """
    output_notebook()

    # Assign colors by task_name
    task_names = sorted({G.nodes[n]["task_name"] for n in G.nodes()})
    palette = Category20[max(3, min(len(task_names), 20))]
    color_map = {name: palette[i % len(palette)] for i, name in enumerate(task_names)}

    # Build full node data — x is column index, y is row position
    nodes = list(G.nodes())
    all_x = [positions[n][0] for n in nodes]
    all_y = [positions[n][1] for n in nodes]
    all_task_colors = [color_map[G.nodes[n]["task_name"]] for n in nodes]
    limit = 15
    def truncate(text):
        return f"{text[:limit] + '...' if len(text) > limit else text}"
    all_labels = [f"{truncate(G.nodes[n]['task_name'])} [{G.nodes[n]['partition']}]" for n in nodes]
    all_memory = [estimate_task_memory(dask_graph[n]) for n in nodes] if calculate_memory else [0] * len(nodes)
    all_reprs = [_format_task_tooltip(n, dask_graph[n], m) for n, m in zip(nodes, all_memory)]

    max_mem = max(all_memory) if all_memory else 1
    min_mem = min(all_memory) if all_memory else 0
    mem_mapper = LinearColorMapper(palette=Viridis256, low=min_mem, high=max_mem)
    all_mem_colors = []
    for mem in all_memory:
        if max_mem == min_mem:
            idx = 128
        else:
            idx = int(255 * (mem - min_mem) / (max_mem - min_mem))
        all_mem_colors.append(Viridis256[min(idx, 255)])

    # Build full edge data with source/dest indices
    node_to_idx = {n: i for i, n in enumerate(nodes)}
    edge_src = []
    edge_dst = []
    for u, v in G.edges():
        edge_src.append(node_to_idx[u])
        edge_dst.append(node_to_idx[v])

    # "Full" data source — holds all data, never rendered directly
    full_node_source = ColumnDataSource(
        data={
            "x": all_x,
            "y": all_y,
            "task_color": all_task_colors,
            "mem_color": all_mem_colors,
            "color": list(all_task_colors),
            "label": all_labels,
            "task_repr": all_reprs,
        }
    )
    full_edge_source = ColumnDataSource(
        data={"src_idx": edge_src, "dst_idx": edge_dst}
    )

    # Initial data — show everything
    init_edge_xs = [[all_x[s], all_x[d]] for s, d in zip(edge_src, edge_dst)]
    init_edge_ys = [[all_y[s], all_y[d]] for s, d in zip(edge_src, edge_dst)]
    # Two arrowheads per edge at 25% and 75%, angle corrected for Bokeh's triangle orientation
    init_arrow_x = []
    init_arrow_y = []
    init_arrow_angle = []
    for s, d in zip(edge_src, edge_dst):
        angle = math.atan2(all_y[d] - all_y[s], all_x[d] - all_x[s]) - math.pi / 2
        init_arrow_x += [all_x[s] + 0.25 * (all_x[d] - all_x[s]), all_x[s] + 0.75 * (all_x[d] - all_x[s])]
        init_arrow_y += [all_y[s] + 0.25 * (all_y[d] - all_y[s]), all_y[s] + 0.75 * (all_y[d] - all_y[s])]
        init_arrow_angle += [angle, angle]

    node_source = ColumnDataSource(
        data={
            "x": list(all_x), "y": list(all_y),
            "task_color": list(all_task_colors),
            "mem_color": list(all_mem_colors),
            "color": list(all_task_colors),
            "label": list(all_labels), "task_repr": list(all_reprs),
        }
    )
    edge_source = ColumnDataSource(data={"xs": init_edge_xs, "ys": init_edge_ys})
    arrow_source = ColumnDataSource(
        data={"x": init_arrow_x, "y": init_arrow_y, "angle": init_arrow_angle}
    )

    # Axis ranges
    n_cols = max(all_x) + 1
    x_pad = 1
    y_pad = 2
    x_range = Range1d(min(all_x) - x_pad, max(all_x) + x_pad)
    y_range = Range1d(min(all_y) - y_pad, max(all_y) + y_pad)

    # Two zoom tools in toolbar: vertical (default) and horizontal (click to switch)
    y_zoom = WheelZoomTool(dimensions="height", description="Vertical Zoom")
    x_zoom = WheelZoomTool(dimensions="width", description="Horizontal Zoom")

    p = figure(
        width=width,
        height=height,
        x_range=x_range,
        y_range=y_range,
        tools=["pan", "box_zoom", "reset", y_zoom, x_zoom],
        active_scroll=y_zoom,
        title="Dask Task Graph",
        lod_threshold=None,
    )
    p.axis.visible = False
    p.grid.visible = False

    # Draw edges
    p.add_glyph(edge_source, MultiLine(xs="xs", ys="ys", line_color="#cccccc", line_width=1.5))

    # Draw arrowheads at edge midpoints
    p.add_glyph(
        arrow_source,
        Scatter(
            x="x",
            y="y",
            angle="angle",
            size=8,
            marker="triangle",
            fill_color="#cccccc",
            line_color=None,
        ),
    )

    # Draw node rectangles (screen-space size)
    rect_glyph = p.add_glyph(
        node_source,
        Rect(
            x="x",
            y="y",
            width=120,
            height=20,
            width_units="screen",
            height_units="screen",
            fill_color="color",
            line_color="#333333",
            line_width=0.5,
        ),
    )

    # Draw node labels
    p.add_glyph(
        node_source,
        Text(
            x="x",
            y="y",
            text="label",
            text_align="center",
            text_baseline="middle",
            text_font_size="8pt",
        ),
    )

    # Hover tooltip on nodes only (HTML content)
    hover = HoverTool(
        tooltips="@task_repr{safe}",
        renderers=[rect_glyph],
    )
    p.add_tools(hover)

    # Memory color bar (initially hidden)
    color_bar = ColorBar(
        color_mapper=mem_mapper,
        title="Memory",
        visible=False,
        width=15,
    )
    p.add_layout(color_bar, "right")

    # Toggle button for memory coloring
    toggle_btn = Button(label="Color by Memory", button_type="default", width=150)

    toggle_js = CustomJS(
        args={
            "node_source": node_source,
            "full_node": full_node_source,
            "color_bar": color_bar,
            "btn": toggle_btn,
        },
        code="""
        const mode = btn.label === 'Color by Memory' ? 'memory' : 'task';

        if (mode === 'memory') {
            btn.label = 'Color by Task';
            color_bar.visible = true;
            // Switch visible source colors
            const n = node_source.data['x'].length;
            const new_colors = [];
            for (let i = 0; i < n; i++) {
                new_colors.push(node_source.data['mem_color'][i]);
            }
            node_source.data['color'] = new_colors;
            // Switch full source colors
            const fn = full_node.data['x'].length;
            const new_full = [];
            for (let i = 0; i < fn; i++) {
                new_full.push(full_node.data['mem_color'][i]);
            }
            full_node.data['color'] = new_full;
        } else {
            btn.label = 'Color by Memory';
            color_bar.visible = false;
            const n = node_source.data['x'].length;
            const new_colors = [];
            for (let i = 0; i < n; i++) {
                new_colors.push(node_source.data['task_color'][i]);
            }
            node_source.data['color'] = new_colors;
            const fn = full_node.data['x'].length;
            const new_full = [];
            for (let i = 0; i < fn; i++) {
                new_full.push(full_node.data['task_color'][i]);
            }
            full_node.data['color'] = new_full;
        }
        node_source.change.emit();
        full_node.change.emit();
        """,
    )
    toggle_btn.js_on_click(toggle_js)

    # JS callback for viewport filtering
    js_path = Path(__file__).parent / "viewport_filter.js"
    callback = CustomJS(
        args={
            "node_source": node_source,
            "edge_source": edge_source,
            "arrow_source": arrow_source,
            "full_node": full_node_source,
            "full_edge": full_edge_source,
            "x_range": p.x_range,
            "y_range": p.y_range,
        },
        code=js_path.read_text(),
    )

    p.x_range.js_on_change("start", callback)
    p.x_range.js_on_change("end", callback)
    p.y_range.js_on_change("start", callback)
    p.y_range.js_on_change("end", callback)

    layout = column(row(toggle_btn), p) if calculate_memory else p
    show(layout)
    return layout


def _format_task_tooltip(key, task, memory_bytes):
    """Build an HTML tooltip string from a Dask task object."""
    import html

    lines = []
    lines.append(f"<b>Key:</b> {html.escape(str(key))}")
    lines.append(f"<b>Memory:</b> {format_bytes(memory_bytes)}")

    # Legacy-style tuple task: (func, arg1, arg2, ...)
    if isinstance(task, tuple) and task and callable(task[0]):
        lines.append("<b>Type:</b> Legacy Task (tuple)")
        func_name = getattr(task[0], "__name__", repr(task[0]))
        lines.append(f"<b>Function:</b> {html.escape(func_name)}")
        for i, arg in enumerate(task[1:]):
            arg_str = repr(arg)
            if len(arg_str) > 200:
                arg_str = arg_str[:200] + "..."
            lines.append(f"<b>Arg {i}:</b> {html.escape(arg_str)}")
        return "<div style='max-width:600px'>" + "<br>".join(lines) + "</div>"

    # Legacy-style list or any other plain Python value (show full repr)
    if not hasattr(task, "key"):
        lines.append(f"<b>Type:</b> Literal ({html.escape(type(task).__name__)})")
        v_str = repr(task)
        if len(v_str) > 500:
            v_str = v_str[:500] + "..."
        lines.append(f"<b>Value:</b> {html.escape(v_str)}")
        return "<div style='max-width:600px'>" + "<br>".join(lines) + "</div>"

    # Alias — points to another key
    if hasattr(task, "target"):
        lines.append("<b>Type:</b> Alias")
        lines.append(f"<b>Alias for:</b> {html.escape(str(task.target))}")
        return "<div style='max-width:600px'>" + "<br>".join(lines) + "</div>"

    # DataNode — holds a literal value
    if not hasattr(task, "func"):
        lines.append("<b>Type:</b> DataNode")
        value = getattr(task, "value", None)
        if value is not None:
            v_str = repr(value)
            if len(v_str) > 500:
                v_str = v_str[:500] + "..."
            lines.append(f"<b>Value:</b> {html.escape(v_str)}")
        return "<div style='max-width:600px'>" + "<br>".join(lines) + "</div>"

    # New-style Task — has function, args, kwargs, dependencies
    lines.append("<b>Type:</b> Task")
    func_name = getattr(task.func, "__name__", repr(task.func))
    lines.append(f"<b>Function:</b> {html.escape(func_name)}")

    for i, arg in enumerate(task.args):
        if type(arg).__name__ in ("TaskRef", "Alias"):
            lines.append(f"<b>Arg {i}:</b> → {html.escape(str(arg))}")
        else:
            arg_str = repr(arg)
            if len(arg_str) > 200:
                arg_str = arg_str[:200] + "..."
            lines.append(f"<b>Arg {i}:</b> {html.escape(arg_str)}")

    for k, v in task.kwargs.items():
        if k == "_meta":
            continue
        if type(v).__name__ in ("TaskRef", "Alias"):
            lines.append(f"<b>{html.escape(k)}:</b> → {html.escape(str(v))}")
        else:
            v_str = repr(v)
            if len(v_str) > 200:
                v_str = v_str[:200] + "..."
            lines.append(f"<b>{html.escape(k)}:</b> {html.escape(v_str)}")

    deps = task.dependencies
    if deps:
        dep_strs = [html.escape(str(d)) for d in deps]
        lines.append(f"<b>Dependencies:</b><br>{'<br>'.join(dep_strs)}")

    return "<div style='max-width:600px'>" + "<br>".join(lines) + "</div>"
