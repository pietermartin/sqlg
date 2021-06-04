import m from 'mithril';
import tippy from "tippy.js";
import $ from 'jquery';

function Input(initialNode) {

    let tooltip;

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
            if (!vnode.attrs.id) {
                throw Error("Input component must have an 'id' attribute. None found!");
            }
            let isTextArea = vnode.attrs.textarea !== undefined && vnode.attrs.textarea;
            let klass = "form-control form-control-sm";
            if (vnode.attrs.class) {
                klass += " " + vnode.attrs.class;
            }
            let attributes = {
                ...vnode.attrs.attrs,
                id: vnode.attrs.id,
                class: klass,
                placeHolder: vnode.attrs.placeHolder ? vnode.attrs.placeHolder : vnode.attrs.id,
                required: !!vnode.attrs.required,
                oninput: vnode.attrs.oninput,
                onkeyup: vnode.attrs.onkeyup,
                value: vnode.attrs.value,
                disabled: vnode.attrs.enabled !== undefined ? !vnode.attrs.enabled :false
            };
            if (vnode.attrs.key) {
                attributes['key'] = vnode.attrs.key;
            }
            if (isTextArea) {
                attributes['rows'] = vnode.attrs.rows ? vnode.attrs.rows : 3;
            } else {
                if (attributes['type'] === undefined) {
                    attributes['type'] = vnode.attrs.type ? vnode.attrs.type : 'text';
                }
                if (attributes['pattern'] === undefined) {
                    attributes['pattern'] = vnode.attrs.pattern ? vnode.attrs.pattern : '.*';
                }
            }
            if (vnode.attrs["data-tippy-content"]) {
                attributes["data-tippy-content"] = vnode.attrs["data-tippy-content"];
            }
            if (vnode.attrs["data-tippy-placement"]) {
                attributes["data-tippy-placement"] = vnode.attrs["data-tippy-placement"];
            }
            if (vnode.attrs["data-tippy-flip"]) {
                attributes["data-tippy-flip"] = vnode.attrs["data-tippy-flip"];
            }
            return isTextArea ? m("textarea", attributes) : m("input", attributes);
        }
    }
}

export default Input;
