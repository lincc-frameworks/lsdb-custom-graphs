const x0 = x_range.start;
const x1 = x_range.end;
const y0 = y_range.start;
const y1 = y_range.end;

const all_x = full_node.data['x'];
const all_y = full_node.data['y'];
const all_color = full_node.data['color'];
const all_task_color = full_node.data['task_color'];
const all_mem_color = full_node.data['mem_color'];
const all_label1 = full_node.data['label_line1'];
const all_repr = full_node.data['task_repr'];
const all_text_color = full_node.data['text_color'];
const all_task_text_color = full_node.data['task_text_color'];
const all_mem_text_color = full_node.data['mem_text_color'];
const all_task_name = full_node.data['task_name'];

const AGGREGATE_CELL_BACKGROUND = '#dddddd';
const AGGREGATE_CELL_TEXT = '#000000';

// Compute grid cell size in data coordinates.
// Use aggregate box dimensions (120x40) plus padding (20x15) so
// aggregate boxes never overlap and always have visible gaps.
const cell_w = (x1 - x0) * 140 / plot_width;
const cell_h = (y1 - y0) * 55 / plot_height;

// Assign every node to a grid cell (needed for edge merging)
const node_cell_key = new Array(all_x.length);
for (let i = 0; i < all_x.length; i++) {
    node_cell_key[i] = Math.floor(all_x[i] / cell_w) + ',' + Math.floor(all_y[i] / cell_h);
}

// Collect visible nodes and group by grid cell
const cells = {};
for (let i = 0; i < all_x.length; i++) {
    if (all_x[i] >= x0 && all_x[i] <= x1 && all_y[i] >= y0 && all_y[i] <= y1) {
        const key = node_cell_key[i];
        if (!cells[key]) {
            cells[key] = [];
        }
        cells[key].push(i);
    }
}

// Build node arrays, aggregating multi-node cells
const out_x = [];
const out_y = [];
const out_color = [];
const out_task_color = [];
const out_mem_color = [];
const out_label1 = [];
const out_label2 = [];
const out_y_off1 = [];
const out_y_off2 = [];
const out_rect_h = [];
const out_repr = [];
const out_text_color = [];
const out_task_text_color = [];
const out_mem_text_color = [];

let has_aggregation = false;
const cell_positions = {};
const MAX_TOOLTIP_LINES = 30;

const cell_keys = Object.keys(cells);
for (let c = 0; c < cell_keys.length; c++) {
    const key = cell_keys[c];
    const indices = cells[key];

    if (indices.length === 1) {
        const i = indices[0];
        cell_positions[key] = { x: all_x[i], y: all_y[i] };
        out_x.push(all_x[i]);
        out_y.push(all_y[i]);
        out_color.push(all_color[i]);
        out_task_color.push(all_task_color[i]);
        out_mem_color.push(all_mem_color[i]);
        out_label1.push(all_label1[i]);
        out_label2.push('');
        out_y_off1.push(0);
        out_y_off2.push(0);
        out_rect_h.push(20);
        out_repr.push(all_repr[i]);
        out_text_color.push(all_text_color[i]);
        out_task_text_color.push(all_task_text_color[i]);
        out_mem_text_color.push(all_mem_text_color[i]);
    } else {
        has_aggregation = true;
        let sum_x = 0, sum_y = 0;
        // Collect unique task names preserving insertion order, and map name->color
        const task_color_map = {};
        const node_labels = [];
        for (let j = 0; j < indices.length; j++) {
            const i = indices[j];
            sum_x += all_x[i];
            sum_y += all_y[i];
            const tn = all_task_name[i];
            if (!task_color_map[tn]) {
                task_color_map[tn] = all_task_color[i];
            }
            node_labels.push(all_label1[i]);
        }
        const avg_x = sum_x / indices.length;
        const avg_y = sum_y / indices.length;
        cell_positions[key] = { x: avg_x, y: avg_y };

        // Line 1: node count
        const line1 = indices.length + ' nodes';

        // Line 2: task names truncated to fit ~18 chars
        const names = Object.keys(task_color_map);
        let line2 = '';
        for (let n = 0; n < names.length; n++) {
            const candidate = line2 ? line2 + ', ' + names[n] : names[n];
            if (candidate.length > 18) {
                line2 = (line2 || names[n].substring(0, 15)) + '...';
                break;
            }
            line2 = candidate;
        }

        // Tooltip with colored squares
        const sq = '<span style="display:inline-block;width:10px;height:10px;margin-right:4px;vertical-align:middle;background:';
        let tooltip = '<div style="max-width:600px">';
        tooltip += '<b>' + indices.length + ' nodes</b><br>';
        tooltip += '<b>Tasks:</b><br>';
        for (let n = 0; n < names.length; n++) {
            tooltip += sq + task_color_map[names[n]] + '"></span>' + names[n] + '<br>';
        }
        tooltip += '<br><b>Nodes:</b><br>';
        const show_count = Math.min(node_labels.length, MAX_TOOLTIP_LINES);
        for (let j = 0; j < show_count; j++) {
            tooltip += node_labels[j] + '<br>';
        }
        if (node_labels.length > MAX_TOOLTIP_LINES) {
            tooltip += '... and ' + (node_labels.length - MAX_TOOLTIP_LINES) + ' more';
        }
        tooltip += '</div>';

        out_x.push(avg_x);
        out_y.push(avg_y);
        out_color.push(AGGREGATE_CELL_BACKGROUND);
        out_task_color.push(AGGREGATE_CELL_BACKGROUND);
        out_mem_color.push(AGGREGATE_CELL_BACKGROUND);
        out_label1.push(line1);
        out_label2.push(line2);
        out_y_off1.push(6);
        out_y_off2.push(-6);
        out_rect_h.push(40);
        out_repr.push(tooltip);
        out_text_color.push(AGGREGATE_CELL_TEXT);
        out_task_text_color.push(AGGREGATE_CELL_TEXT);
        out_mem_text_color.push(AGGREGATE_CELL_TEXT);
    }
}

node_source.data = {
    'x': out_x, 'y': out_y, 'color': out_color,
    'task_color': out_task_color, 'mem_color': out_mem_color,
    'text_color': out_text_color, 'task_text_color': out_task_text_color,
    'mem_text_color': out_mem_text_color,
    'label_line1': out_label1, 'label_line2': out_label2,
    'y_offset_line1': out_y_off1, 'y_offset_line2': out_y_off2,
    'rect_height': out_rect_h,
    'task_repr': out_repr
};

// Edge handling
// box_half_w: data-space distance from node center to right/left edge at current zoom
const box_half_w = (x1 - x0) * 60 / plot_width;

// Scale factors for converting data-space vectors to screen-space for angle computation.
// Screen y is inverted relative to data y, so dy_screen = -dy_data * y_scale.
const x_scale = plot_width / (x1 - x0);
const y_scale = plot_height / (y1 - y0);

function edge_angle(sx, sy, dx, dy) {
    const scr_dx = (dx - sx) * x_scale;
    const scr_dy = -(dy - sy) * y_scale;  // invert: data y-up = screen y-down
    return Math.atan2(scr_dy, scr_dx) - Math.PI / 2;
}

const src = full_edge.data['src_idx'];
const dst = full_edge.data['dst_idx'];
const edge_xs = [];
const edge_ys = [];
const arrow_x = [];
const arrow_y = [];
const arrow_angle = [];

if (has_aggregation) {
    const seen_edges = new Set();
    for (let i = 0; i < src.length; i++) {
        const src_key = node_cell_key[src[i]];
        const dst_key = node_cell_key[dst[i]];
        if (src_key === dst_key) continue;

        const src_pos = cell_positions[src_key];
        const dst_pos = cell_positions[dst_key];

        // Skip only if both endpoints are off-screen
        if (!src_pos && !dst_pos) continue;

        // y-range check using raw positions for off-screen endpoints
        const sy_raw = all_y[src[i]];
        const dy_raw = all_y[dst[i]];
        if ((sy_raw < y0 || sy_raw > y1) && (dy_raw < y0 || dy_raw > y1)) continue;

        const edge_id = src_key + '->' + dst_key;
        if (seen_edges.has(edge_id)) continue;
        seen_edges.add(edge_id);

        // Use aggregate position if in-viewport, raw node position otherwise
        const sx = (src_pos ? src_pos.x : all_x[src[i]]) + box_half_w;
        const sy = src_pos ? src_pos.y : sy_raw;
        const dx = (dst_pos ? dst_pos.x : all_x[dst[i]]) - box_half_w;
        const dy = dst_pos ? dst_pos.y : dy_raw;

        edge_xs.push([sx, dx]);
        edge_ys.push([sy, dy]);

        const angle = edge_angle(sx, sy, dx, dy);
        // Arrow near the visible endpoint; midpoint if both visible
        let frac;
        if (src_pos && dst_pos) frac = 0.5;
        else if (src_pos) frac = 0.2;
        else frac = 0.8;

        arrow_x.push(sx + frac * (dx - sx));
        arrow_y.push(sy + frac * (dy - sy));
        arrow_angle.push(angle);
    }
} else {
    for (let i = 0; i < src.length; i++) {
        const sxc = all_x[src[i]];
        const syc = all_y[src[i]];
        const dxc = all_x[dst[i]];
        const dyc = all_y[dst[i]];

        // Show edge if either endpoint's y is in range (preserves off-screen connections)
        if ((syc < y0 || syc > y1) && (dyc < y0 || dyc > y1)) continue;

        // Full x+y visibility used only for arrow placement
        const src_vis = sxc >= x0 && sxc <= x1 && syc >= y0 && syc <= y1;
        const dst_vis = dxc >= x0 && dxc <= x1 && dyc >= y0 && dyc <= y1;

        // Connect right side of source to left side of destination
        const sx = sxc + box_half_w;
        const sy = syc;
        const dx = dxc - box_half_w;
        const dy = dyc;

        edge_xs.push([sx, dx]);
        edge_ys.push([sy, dy]);

        const angle = edge_angle(sx, sy, dx, dy);

        // Arrow near the visible endpoint; midpoint if both visible
        let frac;
        if (src_vis && dst_vis) frac = 0.5;
        else if (src_vis) frac = 0.2;
        else frac = 0.8;

        arrow_x.push(sx + frac * (dx - sx));
        arrow_y.push(sy + frac * (dy - sy));
        arrow_angle.push(angle);
    }
}

edge_source.data = {'xs': edge_xs, 'ys': edge_ys};
arrow_source.data = {'x': arrow_x, 'y': arrow_y, 'angle': arrow_angle};

node_source.change.emit();
edge_source.change.emit();
arrow_source.change.emit();
