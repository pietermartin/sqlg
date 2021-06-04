import m from 'mithril';

/**
 * produces a bootstrap 'form-group row' with a 'label' and a input[type=?] or a textarea
 * @param initialNode
 * @returns {{view: (function(*))}}
 * @constructor
 */
function FormInputRadioCheckBox(initialNode) {

    let enabled = true;

    return {
        view: function (vnode) {
            if (!vnode.attrs.inputs || !Array.isArray(vnode.attrs.inputs) || vnode.attrs.inputs.length === 0) {
                throw Error("FormInputRadio component must have an 'inputs' attribute which must be an array with at least one entry!");
            }
            if (vnode.attrs.type !== undefined && vnode.attrs.type !== 'radio' && vnode.attrs.type !== 'checkbox') {
                throw Error("FormInputRadio component only supports 'radio' and 'checkbox' types. The default is 'radio'.");
            }
            enabled = vnode.attrs.enabled !== undefined ? vnode.attrs.enabled : true;
            return m("fieldset.form-group", {
                "data-tooltip": vnode.attrs["data-tooltip"] ? vnode.attrs["data-tooltip"] : "",
                "data-tooltip-y": vnode.attrs["data-tooltip-y"] ? vnode.attrs["data-tooltip-y"] : ""
            }, [
                m("div.row", [
                    // backwards compaitibility must ensure no legend attr means it displays ...
                    !vnode.attrs.hideLegend || (!!vnode.attrs.hideLegend && vnode.attrs.hideLegend == false) ? 
                        m("legend", {
                            class: "col-sm-3 col-form-label pt-0"
                        }, vnode.attrs.label ? vnode.attrs.label : "") : 
                        [],
                    m("div", {class: "col-sm-9"}, [
                        vnode.attrs.inputs.map(function (item) {
                            return m("div.form-check", [
                                m("input.form-check-input", {
                                    type: vnode.attrs.type === undefined ? 'radio' : vnode.attrs.type,
                                    id: item.id,
                                    required: !!item.required,
                                    disabled: !enabled,
                                    onchange: function(e) {
                                        if (enabled) {
                                            item.onchange(this);
                                        } else {
                                            e.redraw = false;
                                        }
                                    },
                                    value: item.value,
                                    checked: item.checked

                                }),
                                m("label.form-check-label", {for: item.id}, item.label)
                            ])
                        }),
                        (vnode.attrs.required ?
                            m("div", {class: "invalid-feedback"}, vnode.attrs.invalidFeedback ? vnode.attrs.invalidFeedback : "Please provide a value")
                            : "")
                    ])
                ])
            ])
        }
    }
}

export default FormInputRadioCheckBox;
