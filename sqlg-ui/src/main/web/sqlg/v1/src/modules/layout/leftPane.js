import m from "mithril";
import TopologyTree from "../topology/topologyTree";

function LeftPane(ignore) {

    return {
        view: ({attrs: {state, actions}}) => {
            return m("div.h-100", m(TopologyTree, {state: state, actions: actions}));
        }
    }

}

export default LeftPane;