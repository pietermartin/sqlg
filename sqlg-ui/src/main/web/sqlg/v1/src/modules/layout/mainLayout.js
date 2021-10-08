import m from "mithril";
import "../../components/jquery-splitter/jquery.splitter"
import "../../components/jquery-splitter/jquery.splitter.css"
import LeftRight from "./leftRight";
import Footer from "./footer";
import Split from 'split-grid'
import ToastContainer from "../../components/toast/toastContainer";
import RegularLink from "../../components/form/regularLink";
import {Route} from "../sqlgRoutes";

function MainLayout(ignore) {

    return {
        oncreate: () => {
            Split({
                rowGutters: [{
                    track: 1,
                    element: document.querySelector('.gutter-row-1'),
                }],
                rowMinSize: 20
            })
        },
        view: ({attrs: {state, actions}}) => {
            return [
                m(ToastContainer, {state: state, actions: actions}),
                m("div#main", [
                    m("div#main-body", [
                        m("nav.navbar.navbar-expand-lg.navbar-dark.bg-dark", [
                                m("div.container-fluid", [
                                    m("div.navbar-brand", "Sqlg"),
                                    m("div.d-flex.flex-wrap", [
                                        m("span.navbar-text.me-2.text-info", "username:"),
                                        m("span.navbar-text.me-2.text-primary", state.username),
                                        m("span.navbar-text.me-2.text-info", "url:"),
                                        m("span.navbar-text.text-primary", state.jdbcUrl),
                                        m("button.navbar-toggler", {
                                            type: "button",
                                            "data-bs-toggle": "collapse",
                                            "data-bs-target": "#navbarNavDropdown"
                                        }, m("span.navbar-toggler-icon")),
                                        m("div#navbarNavDropdown.collapse.navbar-collapse", [
                                            m("ul.navbar-nav", [
                                                m("li.nav-item.dropdown", [
                                                    m("a#navbarDropdownMenuLink.nav-link.dropdown-toggle", {
                                                        href: "#",
                                                        "data-bs-toggle": "dropdown"
                                                    }, [
                                                        m("i.fas.fa-user"),
                                                        m("span", " User")
                                                    ]),
                                                    m("ul.dropdown-menu.dropdown-menu-dark.dropdown-menu-end", [
                                                        m("li", m("a.dropdown-item", {
                                                            href: "#", onclick: (e) => {
                                                                actions.navigateTo(Route.SqlgLogin({}));
                                                                actions.reset();
                                                                document.cookie = "SqlgToken=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;";
                                                                e.preventDefault();
                                                                e.stopPropagation();
                                                            }
                                                        }, "Log out"))
                                                    ])
                                                ])
                                            ])
                                        ])
                                    ]),
                                ])
                            ]
                        ),
                        m(LeftRight, {state: state, actions: actions})
                    ]),
                    m("div.gutter-row.gutter-row-1.bg-secondary"),
                    m(Footer, {state: state, actions: actions})
                ])];
        }
    }

}

export default MainLayout;