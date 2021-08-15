import m from "mithril";
import "../../components/jquery-splitter/jquery.splitter"
import "../../components/jquery-splitter/jquery.splitter.css"
import LeftRight from "./leftRight";

function MainLayout(ignore) {


    return {
        view: ({attrs: {state, actions}}) => {
            return m("div#main-body", [
                m("nav.navbar.navbar-dark.bg-dark", [
                        m("div.container-fluid", [
                            m("div.navbar-brand", "Sqlg"),
                            m("div.d-flex.flex-wrap", [
                                m("span.navbar-text.me-2.text-info", "username:"),
                                m("span.navbar-text.me-2.text-primary", state.username),
                                m("span.navbar-text.me-2.text-info", "url:"),
                                m("span.navbar-text.text-primary", state.jdbcUrl)
                            ])
                        ])
                    ]
                ),
                m(LeftRight, {state: state, actions: actions})
            ]);
        }
    }

}

export default MainLayout;