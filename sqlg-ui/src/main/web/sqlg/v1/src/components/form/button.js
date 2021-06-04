import m from 'mithril';
import tippy from "tippy.js";
import $ from 'jquery';

function Button() {

    let enabled = false;
    let iconOnTheLeft = true;
    let tooltip;
    let dataTippyContent;

    return {
        oncreate: function(vnode) {
            let button = $('#' + vnode.attrs.id + '[data-tippy-content]');
            if (button.length > 0) {
                tooltip = tippy(button[0]);
            }
        },
        onremove: function(vnode) {
            if (tooltip !== undefined && tooltip !== null) {
                tooltip.destroy();
            }
        },
        view: function (vnode) {
            enabled = vnode.attrs.enabled === undefined || vnode.attrs.enabled;
            if (vnode.attrs.iconOnTheLeft === false) {
                iconOnTheLeft = false;
            }
            let attrs = {
                ...vnode.attrs.attrs,
                id: vnode.attrs.id,
                type: vnode.attrs.type !== undefined ? vnode.attrs.type : "button",
                class: `${enabled ? 'enabled' : 'disabled'} btn ${vnode.attrs.class ? vnode.attrs.class : ''}`,
                onclick: enabled ? vnode.attrs.onclick : function () {
                    /*noop*/
                }
            };
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
            if (iconOnTheLeft) {
                return m("button",
                    attrs, vnode.attrs.text ? [
                            m('i', {
                                class: (vnode.attrs.icon ?
                                    (['fas ', 'far ', 'fad ', 'fal '].some(e => vnode.attrs.icon.includes(e)) ? vnode.attrs.icon : "fas " + vnode.attrs.icon) : '')
                            },
                            ), " " + vnode.attrs.text
                        ] :
                        m('i', {
                            class: (vnode.attrs.icon ? (['fas ', 'far ', 'fad ', 'fal '].some(e => vnode.attrs.icon.includes(e)) ? vnode.attrs.icon : "fas " + vnode.attrs.icon) : '')
                        })
                );
            } else {
                return m("button", attrs, [
                    m("span", vnode.attrs.text + " "),
                    m('i', {
                        class: (vnode.attrs.icon ? (['fas ', 'far ', 'fad ', 'fal '].some(e => vnode.attrs.icon.includes(e)) ? vnode.attrs.icon : "fas " + vnode.attrs.icon) : '')
                    })
                ]);
            }
        }
    }
}

export default Button;
