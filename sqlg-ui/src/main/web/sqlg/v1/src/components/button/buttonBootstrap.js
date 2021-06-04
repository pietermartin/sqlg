import m from "mithril";

const ButtonBootstrap = {

    view: function (vnode) {
        return m("Button", {class: "btn btn-primary", type: "button", onclick: vnode.attrs.buttonBootstrapOnClick}, vnode.children);
    }

};

export default ButtonBootstrap

