import m from 'mithril';

/**
 * produces a bootstrap 'form-group row' with a 'label' and a input[type=?] or a textarea
 * @param initialNode
 * @returns {{view: (function(*))}}
 * @constructor
 */
function FormInput(initialNode) {

    return {
        view: function (vnode) {
            if (!vnode.attrs.id) {
                throw Error("FormInput component must have an 'id' attribute. None found!");
            }
            let isTextArea = vnode.attrs.textarea !== undefined && vnode.attrs.textarea;
            return m("div", {class: "form-group row"}, [
                m("label", {
                    for: vnode.attrs.id,
                    id: vnode.attrs.id + "_Label",
                    class: "col-sm-3 col-form-label"
                }, vnode.attrs.label ? vnode.attrs.label : ""),
                m("div", {class: "col-sm-9"}, [
                    isTextArea ? m("textarea", {
                            class: "form-control form-control-sm",
                            id: vnode.attrs.id,
                            placeHolder: vnode.attrs.placeHolder ? vnode.attrs.placeHolder : vnode.attrs.id,
                            required: !!vnode.attrs.required,
                            oninput: vnode.attrs.oninput,
                            onkeyup: vnode.attrs.onkeyup,
                            value: vnode.attrs.value,
                            rows: vnode.attrs.rows ? vnode.attrs.rows : 3
                        }) :
                        m("input", {
                            type: vnode.attrs.type ? vnode.attrs.type : 'text',
                            pattern: vnode.attrs.pattern ? vnode.attrs.pattern : '.*',
                            autofocus: vnode.attrs.autofocus ? 'autofocus' : '',
                            class: "form-control form-control-sm",
                            id: vnode.attrs.id,
                            placeHolder: vnode.attrs.placeHolder ? vnode.attrs.placeHolder : vnode.attrs.id,
                            required: !!vnode.attrs.required,
                            oninput: vnode.attrs.oninput,
                            onkeyup: vnode.attrs.onkeyup,
                            value: vnode.attrs.value
                        }),
                    (vnode.attrs.required ?
                        m("div", {class: "invalid-feedback"}, vnode.attrs.invalidFeedback ? vnode.attrs.invalidFeedback : "Please provide a " + vnode.attrs.id)
                        : "")
                ])
            ])
        }
    }
}

export default FormInput;
