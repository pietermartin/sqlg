import m from 'mithril';
import tippy from "tippy.js";
import $ from 'jquery';

function ImportButton() {

    let enabled = false;
    let tooltip;
    let dataTippyContent;

    return {
        oncreate: function (vnode) {
            let button = $('#label_' + vnode.attrs.id + '[data-tippy-content]');
            if (button.length > 0) {
                tooltip = tippy(button[0]);
            }
        },
        onremove: function (vnode) {
            if (tooltip !== undefined && tooltip !== null) {
                tooltip.destroy();
            }
        },
        view: function (vnode) {
            enabled = vnode.attrs.enabled === undefined || vnode.attrs.enabled;
            let onchange = enabled ? vnode.attrs.onchange : function () { /*noop*/
            }
            let attrs = {
                ...vnode.attrs.attrs,
                id: "label_" + vnode.attrs.id,
                for: vnode.attrs.id,
                type: vnode.attrs.type !== undefined ? vnode.attrs.type : "button",
                class: `${enabled ? 'enabled' : 'disabled'} import-btn btn ${vnode.attrs.class ? vnode.attrs.class : ''}`,
                enabled: vnode.attrs.enabled === undefined || vnode.attrs.enabled,
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
            return [
                m("label", attrs,
                    m('i', {
                            class: (vnode.attrs.icon ?
                                (['fas ', 'far ', 'fad ', 'fal '].some(e => vnode.attrs.icon.includes(e)) ? vnode.attrs.icon : "fas " + vnode.attrs.icon) : '')
                        },
                    ), " " + vnode.attrs.text
                ),
                m("input[type=file]", {
                        id: vnode.attrs.id,
                        style: "display: none",
                        value: "",
                        onchange: onchange,
                        title: "Import"
                    },
                )];
        }
    }
}

export default ImportButton;
