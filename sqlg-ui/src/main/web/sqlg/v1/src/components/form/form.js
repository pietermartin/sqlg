import m from 'mithril';

function Form(initialNode) {

    return {
        view: function (vnode) {
            return m("form", {id: vnode.attrs.id, class: "needs-validation", novalidate: "", ... vnode.attrs.attrs}, vnode.children);
        }
    }
}

export default Form;
