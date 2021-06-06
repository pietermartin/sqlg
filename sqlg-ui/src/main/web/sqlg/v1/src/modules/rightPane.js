import m from "mithril";
import SchemaDetail from "./topology/schemaDetail";
import VertexLabelDetail from "./topology/vertexLabelDetails";

function RightPane(ignore) {

    return {
        view: ({attrs: {state, actions}}) => {
            if (state.topologyDetails.type === 'Schema') {
                return m("div.right-pane", m(SchemaDetail, {state: state, actions}));
            } else if (state.topologyDetails.type === 'VertexLabel') {
                return m("div.right-pane", m(VertexLabelDetail, {state: state, actions}));
            } else {
                return m("div.right-pane", m("div"));
            }
        }
    }

}

export default RightPane;