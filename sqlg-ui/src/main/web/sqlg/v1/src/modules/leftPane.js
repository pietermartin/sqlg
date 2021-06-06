import m from "mithril";
import "./_leftPane.scss";
import TopologyTree from "./topology/topologyTree";
import GraphHeader from "./topology/graphHeader";

function LeftPane(ignore) {

    return {
        view: ({attrs: {state, actions}}) => {
            return m("div.left-pane", [
                    m(GraphHeader),
                    m(TopologyTree, {state: state, actions})
                ]
            );
        }
    }

}

export default LeftPane;