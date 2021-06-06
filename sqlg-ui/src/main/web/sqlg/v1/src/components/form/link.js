import m from 'mithril';
import $ from "jquery";
import tippy from "tippy.js";

function Link() {

    let enabled = false;
    let isButton = true;
    let tooltip;
    let dataTippyContent;

    return {
        oncreate: function(vnode) {
            let link = $('#' + vnode.attrs.id + '[data-tippy-content]');
            if (link.length > 0) {
                tooltip = tippy(link[0]);
            }
        },
        onremove: function(vnode) {
            if (tooltip !== undefined && tooltip !== null) {
                tooltip.destroy();
            }
        },
        view: function (vnode) {
            if (vnode.attrs.href === undefined) {
                throw Error("A link button must have a href attr!");
            }
            enabled = vnode.attrs.enabled === undefined || vnode.attrs.enabled;
            isButton = vnode.attrs.isButton === undefined || vnode.attrs.isButton;
            let attrs = {
                ...vnode.attrs.attrs,
                href: vnode.attrs.href,
                title: vnode.attrs.text,
                class: `${enabled ? 'enabled' : 'disabled'} ${isButton ? "btn" : ""} ${vnode.attrs.class ? vnode.attrs.class : ''}`,
                disabled: !enabled,
                onclick: enabled ? vnode.attrs.onclick : function () {
                    /*noop*/
                }
            };
            if (vnode.attrs["id"]) {
                attrs["id"] = vnode.attrs.id;
            }
            if (vnode.attrs["data-tippy-content"]) {
                let previousTippyContent = dataTippyContent;
                dataTippyContent = vnode.attrs["data-tippy-content"];
                attrs["data-tippy-content"] = dataTippyContent;
                if (tooltip !== undefined && previousTippyContent !== dataTippyContent) {
                    tooltip.setContent(dataTippyContent);
                }
            }
            if (vnode.attrs["data-tippy-placement"]) {
                attrs["data-tippy-placement"] = vnode.attrs["data-tippy-placement"];
            }
            return m(m.route.Link,
                attrs, vnode.attrs.text ? [
                        m('i', {class: "fas " + (vnode.attrs.icon ? vnode.attrs.icon : '')}),
                        "   " + vnode.attrs.text
                    ] :
                    m('i', {class: "fas " + (vnode.attrs.icon ? vnode.attrs.icon : '')})
            );
        }
    }
}

export default Link;
