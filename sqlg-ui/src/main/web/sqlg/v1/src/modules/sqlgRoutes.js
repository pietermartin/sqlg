import m from 'mithril';
import MeiosisRouting from "meiosis-routing";

const {createRouteSegments} = MeiosisRouting.state;
const {createMithrilRouter} = MeiosisRouting.routerHelper;

export const Route = createRouteSegments([
    "Sqlg",
    "SqlgLogin"
]);

export const navTo = route => {
    return {nextRoute: () => Array.isArray(route) ? route : [route]};
}

const routeConfig = {
    Sqlg: "/sqlg-ui?treeId&view",
    SqlgLogin: "/sqlg-ui/login",
};
export const router = createMithrilRouter({
    m,
    routeConfig
});
