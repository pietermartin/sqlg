import m from "mithril";
import LeftPane from "./leftPane";
import RightPane from "./rightPane";

function LeftRight(ignore) {

    return {
        oncreate: ({attrs: {}}) => {
            let main = $('#left-right-pane');
            let splitter = main.split({
                orientation: 'vertical',
                limit: 10,
                position: "25%",
                onDrag: function(event) {
                    console.log(splitter.position());
                }
            });
            main.data("splitter", splitter);
        },
        view: ({attrs: {state, actions}}) => {
            return m("div#left-right-pane.h-100", [
                m("div#left-pane", m(LeftPane, {state: state, actions: actions})),
                m("div#right-pane", m(RightPane, {state: state, actions: actions}))
            ]);
        }
    }
}

export default LeftRight;
