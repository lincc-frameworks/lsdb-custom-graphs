const x0 = x_range.start;
const x1 = x_range.end;
const y0 = y_range.start;
const y1 = y_range.end;

const all_x = full_node.data['x'];
const all_y = full_node.data['y'];
const all_color = full_node.data['color'];
const all_label = full_node.data['label'];
const all_repr = full_node.data['task_repr'];

// Filter visible nodes
const vis_x = [];
const vis_y = [];
const vis_color = [];
const vis_label = [];
const vis_repr = [];

for (let i = 0; i < all_x.length; i++) {
    if (all_x[i] >= x0 && all_x[i] <= x1 && all_y[i] >= y0 && all_y[i] <= y1) {
        vis_x.push(all_x[i]);
        vis_y.push(all_y[i]);
        vis_color.push(all_color[i]);
        vis_label.push(all_label[i]);
        vis_repr.push(all_repr[i]);
    }
}

node_source.data = {
    'x': vis_x, 'y': vis_y, 'color': vis_color,
    'label': vis_label, 'task_repr': vis_repr
};

// Filter edges where at least one endpoint is in the y range
const src = full_edge.data['src_idx'];
const dst = full_edge.data['dst_idx'];
const edge_xs = [];
const edge_ys = [];
const arrow_x = [];
const arrow_y = [];
const arrow_angle = [];

for (let i = 0; i < src.length; i++) {
    const sy = all_y[src[i]];
    const dy = all_y[dst[i]];
    if ((sy >= y0 && sy <= y1) || (dy >= y0 && dy <= y1)) {
        const sx = all_x[src[i]];
        const dx = all_x[dst[i]];
        edge_xs.push([sx, dx]);
        edge_ys.push([sy, dy]);

        // Arrowhead at midpoint, pointing from src to dst
        arrow_x.push((sx + dx) / 2);
        arrow_y.push((sy + dy) / 2);
        arrow_angle.push(Math.atan2(dy - sy, dx - sx));
    }
}

edge_source.data = {'xs': edge_xs, 'ys': edge_ys};
arrow_source.data = {'x': arrow_x, 'y': arrow_y, 'angle': arrow_angle};

node_source.change.emit();
edge_source.change.emit();
arrow_source.change.emit();
