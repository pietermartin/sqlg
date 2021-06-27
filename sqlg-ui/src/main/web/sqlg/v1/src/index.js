import m from "mithril";
import "./sqlg.scss"
import "bootstrap";
import Sqlg from "./modules/sqlg";
import PubSub from "./utils/pubSub";

PubSub.init();

m.route.prefix = '#!';
m.route(document.body, "/sqlg-ui",
    {
        "/sqlg-ui": {
            onmatch: function(args, requestedPath) {
                $.Topic('/sqlg-ui').publish({args, requestedPath});
            },
            render: function () {
                return m(Sqlg);
            }
        }
    }
);
