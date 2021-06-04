import m from 'mithril';
import tippy from "tippy.js";

function Icon(initialNode) {

    let tooltip;
    let allowHtml = false;

    return {
        oncreate: function (vnode) {
            if (vnode.attrs.id !== undefined && vnode.attrs["data-tippy-content"] !== undefined) {
                let icon = document.querySelector('#' + vnode.attrs.id + '[data-tippy-content]');
                if (icon !== null) {
                    if (allowHtml === true) {
                        tooltip = tippy(icon, {allowHTML: allowHtml});
                    } else {
                        tooltip = tippy(icon);
                    }
                }
            }
        },
        onremove: function (vnode) {
            if (tooltip !== undefined && tooltip !== null) {
                tooltip.destroy();
            }
        },
        view: function (vnode) {
            let attrs = {
                ...vnode.attrs.attrs,
                id: vnode.attrs.id,
                class: vnode.attrs.class ? vnode.attrs.class : '',
                onclick: vnode.attrs.onclick !== undefined ? vnode.attrs.onclick : function () {
                    /*noop*/
                }
            };
            if (vnode.attrs["data-tippy-content"]) {
                attrs["data-tippy-content"] = vnode.attrs["data-tippy-content"];
            }
            if (vnode.attrs["data-tippy-placement"]) {
                attrs["data-tippy-placement"] = vnode.attrs["data-tippy-placement"];
            }
            if (vnode.attrs["data-tippy-allowHtml"]) {
                allowHtml = vnode.attrs["data-tippy-allowHtml"];
            }
            if (vnode.attrs["data-tippy-trigger"]) {
                attrs["data-tippy-trigger"] = vnode.attrs["data-tippy-trigger"];
            }
            return m("i", attrs);
        }
    }

}

export default Icon;
