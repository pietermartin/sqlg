import m from 'mithril';
import $ from "jquery";
import _ from "underscore";
import Icon from "../form/icon";

function TabGroup(initialVnode) {

    let active = 0;
    let contextMenuTab = {label: ''};
    let showContextMenu = false;
    let top;
    let left;

    function setActive(vnode) {
        let index = 0;
        for (let i = 0; i < vnode.attrs.tabs.length; i++) {
            let tab = vnode.attrs.tabs[i];
            if (tab.activeParam) {
                if (m.route.param(tab.activeParam) === undefined && tab.defaultParam === undefined) {
                    throw Error("'" + tab.activeParam + "' must be present in the route. route = " + m.route.get());
                }
                if (tab.param[tab.activeParam] === undefined) {
                    throw Error("'" + tab.activeParam + "' must be a tab param." + tab.param);
                }
                let activeParamValue = m.route.param(tab.activeParam);
                if (activeParamValue !== undefined) {
                    activeParamValue = activeParamValue.toString();
                } else {
                    activeParamValue = tab.defaultParam;
                }
                if (activeParamValue === decodeURIComponent(tab.param[tab.activeParam].toString())) {
                    index = i;

                    let tabId = 'tab' + i + "-" + tab.label.replace(/ /g, "_");
                    if (document.getElementById(tabId)) {
                        let theActiveTab = document.getElementById(tabId);
                        let tabContainer = theActiveTab.parentNode;
                        let scrollAmount = 0;
                        if ((theActiveTab.offsetLeft > tabContainer.offsetWidth / 2)) {
                            scrollAmount = theActiveTab.offsetLeft
                        } else if ((theActiveTab.offsetLeft < tabContainer.offsetWidth / 2)) {
                            scrollAmount = (-theActiveTab.offsetLeft)
                        }
                        tabContainer.scrollTo({
                            top: 0,
                            left: scrollAmount,
                            behavior: 'smooth'
                        });
                    }

                }
            } else {
                for (let property in tab.param) {
                    if (tab.param.hasOwnProperty(property)) {
                        if (m.route.param(property) === tab.param[property]) {
                            index = i;
                            break;
                        }
                    }
                }
            }
        }
        active = index;
    }

    let tab = function (vnode, tab, i) {
        if (tab.isAdd) {
            return m('.tab', {
                key: "tab" + i,
                onclick: function (e) {
                    if (tab.plusCallBack) {
                        tab.plusCallBack(e);
                    } else {
                        e.redraw = false;
                    }
                }
            }, [
                m("label", m("i.fas fa-plus"))
            ])
        } else {
            // noinspection JSUnusedGlobalSymbols
            return m('.tab', {
                key: "tab" + i,
                id: "tab" + i + "-" + tab.label.replace(/ /g, "_"),
                class: active === i ? 'active' : '',
                onclick: (e) => {
                    showContextMenu = false;
                    if (tab.external) {
                        location.href = tab.route;
                        return;
                    }
                    active = i;
                    if (tab.route) {
                        m.route.set(tab.route, !!vnode.attrs.preserveOtherParams ? _.extend({}, m.route.param(), tab.param) : tab.param, {replace: false});
                    } else if (tab.param) {
                        m.route.set(m.route.get(), !!vnode.attrs.preserveOtherParams ? _.extend({}, m.route.param(), tab.param) : tab.param, {replace: true});
                    }
                    if (vnode.attrs.onclick !== undefined) {
                        vnode.attrs.onclick(tab);
                    }
                    e.redraw = false;
                    e.preventDefault();
                    e.stopPropagation();
                },
                oncontextmenu: (e) => {
                    showContextMenu = true;
                    left = e.pageX;
                    top = e.pageY;
                    contextMenuTab.label = tab.label;
                    $(document).on('click contextmenu', function (event) {
                        if (showContextMenu && !$(event.target).closest('#tab-' + tab.label).length) {
                            showContextMenu = false;
                            m.redraw();
                            e.preventDefault();
                            e.stopPropagation();
                        }
                    });
                    e.preventDefault();
                    e.stopPropagation();
                }
            }, [
                m('label', [
                    tab.loading ? m("i.fas fa-circle.tab-busy-loading") : '',
                    !tab.loading ? m("i.fas tab-busy-loading") : '',
                    tab.label,
                    vnode.attrs.closeTab ?
                        m(Icon, {
                            id: "closeTabId",
                            class: "fad fa-times",
                            "data-tippy-content": "Close",
                            "data-tippy-placement": "top",
                            onclick: (e) => {
                                vnode.attrs.closeTab(tab);
                                e.redraw = false;
                                e.stopPropagation();
                            }
                        })
                        : '',
                    active === i && vnode.attrs.refreshTab ?
                        m(Icon, {
                            id: "refreshTabId",
                            class: "fad fa-sync",
                            "data-tippy-content": "Refresh",
                            "data-tippy-placement": "top",
                            onclick: function (e) {
                                vnode.attrs.refreshTab(tab);
                            },
                        })
                        : '',
                ]),
                vnode.attrs.contextMenu && contextMenuTab.label === tab.label ?
                    vnode.attrs.contextMenu(showContextMenu, style, tab, function () {
                        showContextMenu = false;
                    }) :
                    ""
            ]);
        }
    };

    let style;

    return {
        onremove: function (vnode) {
        },
        view: function (vnode) {

            let tabs;
            if (vnode.attrs.plusCallBack) {
                tabs = _.union(vnode.attrs.tabs, [{isAdd: true, plusCallBack: vnode.attrs.plusCallBack}]);
            } else {
                tabs = vnode.attrs.tabs;
            }

            setActive(vnode);
            style = showContextMenu ? `display: block; top: ${top}px; left: ${left}px; position: fixed;` : "";
            return m("div.tab-container",
                m('div.tabs',
                    tabs.map(function (t, index) {
                        return tab(vnode, t, index);
                    })
                ),
                m("div.tab-pane", vnode.children)
            );
        }
    }
}

export default TabGroup;

