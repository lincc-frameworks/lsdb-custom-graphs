const mode = btn.label === 'Color by Memory' ? 'memory' : 'task';

if (mode === 'memory') {
    btn.label = 'Color by Task';
    color_bar.visible = true;
    // Switch visible source colors and text colors
    const n = node_source.data['x'].length;
    const new_colors = [];
    const new_tc = [];
    for (let i = 0; i < n; i++) {
        new_colors.push(node_source.data['mem_color'][i]);
        new_tc.push(node_source.data['mem_text_color'][i]);
    }
    node_source.data['color'] = new_colors;
    node_source.data['text_color'] = new_tc;
    // Switch full source colors
    const fn = full_node.data['x'].length;
    const new_full = [];
    const new_full_tc = [];
    for (let i = 0; i < fn; i++) {
        new_full.push(full_node.data['mem_color'][i]);
        new_full_tc.push(full_node.data['mem_text_color'][i]);
    }
    full_node.data['color'] = new_full;
    full_node.data['text_color'] = new_full_tc;
} else {
    btn.label = 'Color by Memory';
    color_bar.visible = false;
    const n = node_source.data['x'].length;
    const new_colors = [];
    const new_tc = [];
    for (let i = 0; i < n; i++) {
        new_colors.push(node_source.data['task_color'][i]);
        new_tc.push(node_source.data['task_text_color'][i]);
    }
    node_source.data['color'] = new_colors;
    node_source.data['text_color'] = new_tc;
    const fn = full_node.data['x'].length;
    const new_full = [];
    const new_full_tc = [];
    for (let i = 0; i < fn; i++) {
        new_full.push(full_node.data['task_color'][i]);
        new_full_tc.push(full_node.data['task_text_color'][i]);
    }
    full_node.data['color'] = new_full;
    full_node.data['text_color'] = new_full_tc;
}
node_source.change.emit();
full_node.change.emit();
