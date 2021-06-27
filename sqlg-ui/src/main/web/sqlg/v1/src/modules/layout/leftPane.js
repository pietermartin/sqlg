import m from "mithril";
import TopologyTree from "../topology/topologyTree";

function LeftPane(ignore) {

    return {
        view: ({attrs: {state, actions}}) => {
            return m("div.left-pane", [
                    m("nav.navbar.navbar-dark.bg-dark",
                        m("div.container-fluid", [
                            m("div.navbar-brand", "Sqlg")
                        ])
                    ),
                    m(TopologyTree, {state: state, actions})
                ]
            );
        }
    }

}

export default LeftPane;