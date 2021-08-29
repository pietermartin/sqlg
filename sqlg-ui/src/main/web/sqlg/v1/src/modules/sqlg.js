import m from "mithril";
import MeiosisRouting from "meiosis-routing";
import MainLayout from "./layout/mainLayout";
import SqlgModel from "./sqlgModel";
import {Route} from "./sqlgRoutes";

function Sqlg(ignore) {

    let states, actions;
    const {Routing} = MeiosisRouting.state;

    let onmatch = (message) => {
        actions.navigateTo(Route.Sqlg(message.args));
    };

    return {
        oninit: () => {
            $.Topic('/sqlg-ui').subscribe(onmatch);
            ({states, actions} = SqlgModel());
            let params = m.route.param();
            let {treeId, view} = params;
            actions.navigateTo(Route.Sqlg({treeId: treeId, view: view !== undefined ? view : states().selectedTab}));
            actions.retrieveGraphData();
            actions.retrieveTopologyTree(params.treeId);
        },
        onremove: () => {
            $.Topic('/sqlg-ui').unsubscribe(onmatch);
        },
        view: () => {
            let state = states();
            if (state.messages.length > 0) {
                setTimeout(() => {
                    for (const message of state.messages) {
                        // CmMithrilGlobal.toasts.push({
                        //     type: message.type,
                        //     message: message.message,
                        //     header: SCREEN_NAME,
                        //     autohide: message.autohide !== undefined ? message.autohide : true
                        // });
                    }
                    m.redraw();
                }, 0);
            }
            return m(MainLayout, {
                width: "25%", state: state, actions: actions
            });
        }
    }

}

export default Sqlg;