window.DASH_API = localStorage.getItem('dashApi') || '';
window.dashUrl = function (path) { return window.DASH_API + path; };
window.dashWsUrl = function () {
    if (!window.DASH_API) {
        const proto = window.location.protocol === 'https:' ? 'wss' : 'ws';
        return proto + '://' + window.location.host + '/ws';
    }
    return window.DASH_API.replace(/^http/, 'ws') + '/ws';
};
