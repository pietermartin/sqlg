import m from "mithril";
import $ from "jquery";
import "./../gridstack.js/gridstack";
import "./../gridstack.js/gridstack.jQueryUI";
import "./../gridstack.js/gridstack.css";

function GridStack(initialVnode) {

    let gridStack;

    return {
        oninit: function (vnode) {
        },
        oncreate: function (vnode) {
            console.log("GridStack.oncreate");
            let $grid = $("#" + vnode.attrs.id);
            $grid.gridstack(vnode.attrs.options);
            gridStack = $grid.data("gridstack");
            //pass it to the parent
            vnode.attrs.onCreateGridStack(gridStack);
        },
        onremove: function (vnode) {
            gridStack.destroy();
        },
        view: function (vnode) {
            return m("div.container-fluid", [
                m("div.grid-stack", {id: vnode.attrs.id}, vnode.children)
            ]);
        }
    }
}

export default GridStack;
