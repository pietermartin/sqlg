const SqlgGlobal = {

    CONTEXT: "/sqlg/data/v1/",
    HASH_BANG: "#!",

    protocol: window.location.protocol,
    hostname: window.location.hostname,
    port: window.location.port,
    url: window.location.protocol + '//' + window.location.hostname + (window.location.port ? ':' + window.location.port : ''),
    username: "Prof. Dolittle",
    roles: [],
    toasts: [],
    toast: {
        type: "",
        message: "",
        display: false
    },
    bundles: {},
    loadedModules: {},
    isEtlServerUp: false,
    isCmJettyUp: false,


};

export default SqlgGlobal;