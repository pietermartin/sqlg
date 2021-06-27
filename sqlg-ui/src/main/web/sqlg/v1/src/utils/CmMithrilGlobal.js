import PubSub from "./pubSub"

const CmMithrilGlobal = {

    CONTEXT: "/networkx/v3/",
    HASH_BANG: "#!",
    ETLSERVER: "/etlserverx/v3/",
    NETVU: "/netvu/v3/",
    TRANSMISSION : "/tx/v3/",
    ANGULAR_CONTEXT: "/networks#/",
    CMTOKEN_COOKIE: "CMToken",

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

    message: function (message) {
        return message;
    },

};
// Websocket.connect();
PubSub.init();

export default CmMithrilGlobal;
