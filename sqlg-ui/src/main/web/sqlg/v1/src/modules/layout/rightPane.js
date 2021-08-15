import m from "mithril";
import SchemaDetail from "../topology/schemaDetail";
import LeftRight from "./leftRight";
import LeftPane from "./leftPane";
import AbstractLabelDetail from "../topology/abstractLabelDetails";
import {Tab} from "bootstrap";

function RightPane(ignore) {

    let activeTab;

    return {
        oncreate: ({attrs: {state, actions}}) => {
            let triggerTabList = [].slice.call(document.querySelectorAll('#nav-bar-ul button[data-bs-toggle="tab"]'))
            triggerTabList.forEach(function (triggerEl) {
                triggerEl.addEventListener('shown.bs.tab', function (event) {
                    activeTab = event.target.id;
                    actions.setActiveTab(event.target.id);
                    m.redraw();
                });
                let tabTrigger = new Tab(triggerEl)
                if (triggerEl.id === state.selectedTab) {
                    tabTrigger.show()
                }
            });
        },
        view: ({attrs: {state, actions}}) => {
            let topology;
            if (state.topologyDetails.type === 'Schema') {
                topology = m(SchemaDetail, {state: state, actions});
            } else if (state.topologyDetails.type === 'AbstractLabel') {
                topology = m(AbstractLabelDetail, {state: state, actions});
            } else {
                topology = m("div")
            }
            if (activeTab !== undefined && activeTab !== state.selectedTab) {
                let triggerEl = document.querySelector('#' + state.selectedTab)
                let tab = Tab.getInstance(triggerEl)
                tab.show();
            }
            return [
                m("nav.navbar.navbar-dark.bg-info", [
                        m("div.container-fluid", [
                            m("ul#nav-bar-ul.nav.nav-pills.ms-2", {role: "tablist"}, [
                                m("li.nav-item", {role: "presentation"},
                                    m("button.nav-link", {
                                        id: "db",
                                        "data-bs-toggle": "tab",
                                        "data-bs-target": "#dbPanel",
                                        type: "button",
                                        role: "tab",
                                        "aria-controls": "db",
                                        "aria-current": "db"
                                    }, "db")
                                ),
                                m("li.nav-item", {role: "presentation"},
                                    m("button.nav-link", {
                                        id: "topology",
                                        "data-bs-toggle": "tab",
                                        "data-bs-target": "#topologyPanel",
                                        type: "button",
                                        role: "tab",
                                        "aria-controls": "topology",
                                        "aria-selected": "true"
                                    }, "topology")
                                ),
                                m("li.nav-item", {role: "presentation"},
                                    m("button.nav-link", {
                                        id: "query",
                                        "data-bs-toggle": "tab",
                                        "data-bs-target": "#queryPanel",
                                        type: "button",
                                        role: "tab",
                                        "aria-controls": "query",
                                        "aria-selected": "true"
                                    }, "query")
                                )
                            ]),
                            m("a.navbar-brand.position-absolute.start-50.text-center", state.topologyDetails.abstractLabel.label),
                        ])
                    ]
                ),
                m("div#main.tab-content", [
                    m("div.tab-pane.fade", {
                        id: "dbPanel",
                        role: "tabpanel",
                        "aria-labelled-by": "db-tab"
                    }, "db"),
                    m("div.tab-pane.fade", {
                        id: "topologyPanel",
                        role: "tabpanel",
                        "aria-labelled-by": "topology-tab"
                    }, [topology]),
                    m("div.tab-pane.fade", {
                        id: "queryPanel",
                        role: "tabpanel",
                        "aria-labelled-by": "query-tab"
                    }, "query")
                ])
            ];
        }
    }

}

export default RightPane;
