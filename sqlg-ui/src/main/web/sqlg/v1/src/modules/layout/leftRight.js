import m from "mithril";
import LeftPane from "./leftPane";
import RightPane from "./rightPane";
import Split from "split-grid";

function LeftRight(ignore) {

    return {
        oncreate: ({attrs: {}}) => {
            Split({
                columnGutters: [{
                    track: 1,
                    element: document.querySelector('.gutter-col-1'),
                }]
            })
        },
        view: ({attrs: {state, actions}}) => {
            return m("div#left-right-pane", [
                m("div#left-pane", m(LeftPane, {state: state, actions: actions})),
                m("div.gutter-col.gutter-col-1.bg-secondary"),
                m("div#right-pane", m(RightPane, {state: state, actions: actions}))
            ]);
        }
    }
}

export default LeftRight;
