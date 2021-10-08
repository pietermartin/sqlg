import m from "mithril";
import MeiosisRouting from "meiosis-routing";
import MainLayout from "./layout/mainLayout";
import SqlgModel from "./sqlgModel";
import {Route} from "./sqlgRoutes";
import LoginForm from "./auth/loginForm";

function Sqlg(ignore) {

    let states, actions;
    const {Routing} = MeiosisRouting.state;

    let signedIn = () => {
        return document.cookie.indexOf("SqlgToken") !== -1;
    };

    let onmatch = (message) => {
        actions.navigateTo(Route.Sqlg(message.args));
    };

    return {
        oninit: () => {
            $.Topic('/sqlg-ui').subscribe(onmatch);
            ({states, actions} = SqlgModel());
            let params = m.route.param();
            let {treeId, view} = params;
            if (signedIn()) {
                actions.navigateTo(Route.Sqlg({treeId: treeId, view: view !== undefined ? view : states().selectedTab}));
            } else {
                actions.navigateTo(Route.SqlgLogin({}));
            }
        },
        oncreate: () => {
            let state = states();
            let params = m.route.param();
            let {treeId, view} = params;
            if (signedIn()) {
                actions.retrieveGraphData();
                actions.retrieveTopologyTree(treeId);
            }
        },
        onremove: () => {
            $.Topic('/sqlg-ui').unsubscribe(onmatch);
        },
        view: () => {
            let state = states();
            if (!signedIn()) {
                actions.navigateTo(Route.SqlgLogin({}));
                return m(LoginForm, {state: state, actions: actions})
            } else {
                return m(MainLayout, {state: state, actions: actions});
            }
        }
    }

}

export default Sqlg;