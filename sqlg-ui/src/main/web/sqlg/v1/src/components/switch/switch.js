import m from 'mithril';
import tippy from "tippy.js";
import $ from 'jquery';

function Switch(ignore) {

    let checked = false;
    let tooltip;

    return {
        oninit: function (vnode) {
            if (vnode.attrs.checked) {
                checked = vnode.attrs.checked;
            }
        },
        oncreate: function (vnode) {
            let toggle = $('#' + vnode.attrs.id + "-span" + '[data-tippy-content]');
            if (toggle.length > 0) {
                tooltip = tippy(toggle[0]);
            }
        },
        onremove: function (vnode) {
            if (tooltip !== undefined && tooltip !== null) {
                tooltip.destroy();
            }
        },
        view: function (vnode) {
            if (!vnode.attrs.id) {
                throw Error("switchId must be defined for the Switch control.");
            }
            // if (vnode.attrs.refresh) {
            checked = vnode.attrs.checked;
            // }
            let enabled = vnode.attrs.enabled === undefined || vnode.attrs.enabled;

            let spanAttributes = {id: vnode.attrs.id + "-span"};
            if (vnode.attrs.class) {
                spanAttributes['class'] = vnode.attrs.class;
            }
            if (vnode.attrs['data-tippy-content']) {
                spanAttributes['data-tippy-content'] = vnode.attrs['data-tippy-content'];
            }
            if (vnode.attrs['data-tippy-placement']) {
                spanAttributes['data-tippy-placement'] = vnode.attrs['data-tippy-placement'];
            }
            if (vnode.attrs['data-tippy-flip']) {
                spanAttributes['data-tippy-flip'] = vnode.attrs['data-tippy-flip'];
            }
            return m("div.form-check.form-switch", spanAttributes, [
                m("input.form-check-input[id='" + vnode.attrs.id + "'][type='checkbox']", {
                    checked: checked,
                    tabindex: enabled ? 0 : -1,
                    onclick: function (e) {
                        if (enabled) {
                            checked = !checked;
                            vnode.attrs.toggle(checked, e);
                        } else {
                            e.preventDefault();
                            e.stopPropagation();
                            e.redraw = false;
                        }
                    }
                }),
                m("label.align-text-bottom.form-check-label[for='" + vnode.attrs.id + "']", {
                    class: enabled ? 'enabled' : 'disabled',
                }, vnode.attrs.switchText)
            ])
        }
    }

}

export default Switch;
