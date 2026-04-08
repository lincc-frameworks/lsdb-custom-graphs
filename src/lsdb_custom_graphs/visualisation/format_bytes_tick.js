var units = ['B', 'KB', 'MB', 'GB', 'TB'];
var n = Math.abs(tick);
for (var i = 0; i < units.length; i++) {
    if (n < 1024) {
        return (units[i] === 'B') ? n + ' ' + units[i] : n.toFixed(1) + ' ' + units[i];
    }
    n /= 1024;
}
return n.toFixed(1) + ' TB';
