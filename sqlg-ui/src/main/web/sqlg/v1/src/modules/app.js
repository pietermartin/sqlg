import m from "mithril";
import MainLayout from "./mainLayout";
import LeftPane from "./leftPane";
import RightPane from "./rightPane";

function App(ignore) {

    return {
        view: function (vnode) {
            return m(MainLayout, {
                width: '25%',
                leftPane: m(LeftPane),
                rightPane: m(RightPane)
            });
        }
    }

}

export default App;