import m from 'mithril';
import Button from "./button";

function FormInputCustomWithButton() {

    return {
        view: function (vnode) {
            if (!vnode.attrs.input) {
                throw Error("FormInputCustomWithButton component must have an 'input' attribute. None found!");
            }
            if (!vnode.attrs.input.attrs.id) {
                throw Error("FormInputCustomWithButton component must have an 'input' with an 'id' attribute. None found!");
            }
            let id = vnode.attrs.input.attrs.id;
            let hide = vnode.attrs.hide ? "d-none" : "";
            return m("div", {class: "form-group row " + hide}, [
                m("label", {
                    for: id,
                    id: id + "_Label",
                    class: "col-sm-3 col-form-label pull-right"
                }, vnode.attrs.label ? vnode.attrs.label : ""),
                m("div", {class: "col-sm-7"}, [
                    vnode.attrs.input,
                    (vnode.attrs.input.attrs.required ?
                        m("div", {class: "invalid-feedback", style: `display: ${vnode.attrs.input.attrs.valid === undefined ||  vnode.attrs.input.attrs.valid ? 'none' : 'block'}`}, vnode.attrs.invalidFeedback ? vnode.attrs.invalidFeedback : "Please provide a " + id)
                        : "")
                ]),
                m("div", {class: "col-sm-2"}, [
                    m(Button, {
                        class: "btn-primary btn-block btn-sm",
                        icon: vnode.attrs.buttonIcon,
                        text: vnode.attrs.buttonText,
                        enabled: vnode.attrs.buttonEnabled,
                        attrs: vnode.attrs.buttonAttrs,
                        onclick: vnode.attrs.buttonOnclick
                    })
                ]),
            ])
        },
    }
}

export default FormInputCustomWithButton;
