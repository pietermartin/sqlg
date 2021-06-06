import m from "mithril";
import TopologyManager from "./TopologyManager";

function GraphHeader(ignore) {

    let jdbcUrl;
    let username;

    return {
        oninit: (vnode) => {
            TopologyManager.retrieveGraphData((data) => {
                jdbcUrl = data.jdbcUrl;
                username = data.username;
            }, (e) => {
                jdbcUrl = "Error";
                username = "Error";
            })
        },
        view: (vnode) => {
            return m("div.header.bg-light", [
                m("label.ml-1.mt-auto", "jdbc.url:"),
                m("label.mt-auto", jdbcUrl),
                m("label.ml-1.mt-auto", "username:"),
                m("label.mt-auto", username)
            ]);
        }
    }

}

export default GraphHeader;