import m from "mithril";
import "./sqlg.scss"
import App from "./modules/app";

m.route.prefix = '#!';
m.route(document.body, "/sqlg",
    {
        "/sqlg": {
            render: function () {
                return m(App);
            }
        }
    }
);
