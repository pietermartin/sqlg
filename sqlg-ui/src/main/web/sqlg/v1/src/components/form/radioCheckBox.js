import m from 'mithril';

function RadioCheckBox(initialNode) {


    return {
        view: function (vnode) {
            if (vnode.attrs.type !== undefined && vnode.attrs.type !== 'radio' && vnode.attrs.type !== 'checkbox') {
                throw Error("RadioCheckBox component only supports 'radio' and 'checkbox' types. The default is 'radio'.");
            }
            let item = vnode.attrs.input;
            let enabled = item.enabled === undefined || item.enabled === true;
            let attributes = {
                id: item.id,
                class: item.class,
                type: vnode.attrs.type === undefined ? 'radio' : vnode.attrs.type,
                required: !!item.required,
                disabled: !enabled,
                onchange: function (e) {
                    if (enabled) {
                        item.onchange(item);
                    } else {
                        e.redraw = false;
                    }
                },
                value: item.value,
                checked: item.checked
            };
            if (item["data-tippy-content"]) {
                attributes["data-tippy-content"] = item["data-tippy-content"];
            }
            if (item["data-tippy-placement"]) {
                attributes["data-tippy-placement"] = item["data-tippy-placement"];
            }
            if (item["data-tippy-flip"]) {
                attributes["data-tippy-flip"] = item["data-tippy-flip"];
            }
            return m("input", attributes)
        }
    }
}

export default RadioCheckBox;
