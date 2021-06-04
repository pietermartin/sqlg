import m from 'mithril';

function FormInputCustom() {

    return {
        view: function (vnode) {
            if (!vnode.attrs.input) {
                throw Error("FormInputCustom component must have an 'input' attribute. None found!");
            }
            if (!vnode.attrs.input.attrs.id) {
                throw Error("FormInputCustom component must have an 'input' with an 'id' attribute. None found!");
            }
            let id = vnode.attrs.input.attrs.id;
            let hide = vnode.attrs.hide ? "d-none" : "";
            let hasKey = vnode.attrs.input['key'] !== undefined;

            return m("div", {class: "form-group row " + hide}, [
                m("label", {
                    for: id,
                    id: id + "_Label",
                    class: "col-sm-3 col-form-label"
                }, vnode.attrs.label ? vnode.attrs.label : ""),
                m("div", {class: "col-sm-9"}, [
                    vnode.attrs.input,
                    (vnode.attrs.input.attrs.required ?
                        (!hasKey ?
                            m("div", {
                                class: "invalid-feedback",
                                style: `display: ${vnode.attrs.input.attrs.valid === undefined || vnode.attrs.input.attrs.valid ? 'none' : 'block'}`
                            }, vnode.attrs.invalidFeedback ? vnode.attrs.invalidFeedback : "Please provide a " + id)
                            :
                            m("div", {
                                key: "div_" + id,
                                class: "invalid-feedback",
                                style: `display: ${vnode.attrs.input.attrs.valid === undefined || vnode.attrs.input.attrs.valid ? 'none' : 'block'}`
                            }, vnode.attrs.invalidFeedback ? vnode.attrs.invalidFeedback : "Please provide a " + id))
                        : "")

                ])
            ])
        },
    }
}

export default FormInputCustom;
