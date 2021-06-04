import m from "mithril";
import "./mainLayout.scss";
import "kendo-ui-core/js/kendo.splitter"

function MainLayout(ignore) {

    let splitter;

    return {
        oncreate: function (vnode) {
            splitter = $("#main").kendoSplitter({
                orientation: "horizontal",
                resize: vnode.attrs.resize ? vnode.attrs.resize : '',
                panes: [
                    {
                        resizable: true,
                        collapsible: true,
                        size: vnode.attrs.width
                    },
                    {
                        resizable: true,
                        collapsible: false
                    }
                ]
            }).data("kendoSplitter");
        },
        view: function (vnode) {
            return m("div#main", {class: vnode.attrs.mainBodyClass}, [
                m("div", {id: "left-pane"}, vnode.attrs.leftPane),
                m("div", {id: "right-pane"}, vnode.attrs.rightPane)
            ]);
        }
    }

}

export default MainLayout;