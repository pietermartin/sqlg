import m from 'mithril';
import $ from "jquery";

function Modal(ignore) {

    return {
        oninit: function (vnode) {
        },
        oncreate: function (vnode) {
            let id = $('#' + vnode.attrs.id);
            id.on('show.bs.modal', function (e) {
                if (vnode.attrs.onShow) {
                    vnode.attrs.onShow(e);
                }
            });
            id.on('shown.bs.modal', function (e) {
                if (vnode.attrs.onShown) {
                    vnode.attrs.onShown(e);
                }
            });
            id.on('hide.bs.modal', function (e) {
                if (vnode.attrs.onHide) {
                    vnode.attrs.onHide(e);
                }
            });
            id.on('hidden.bs.modal', function (e) {
                if (vnode.attrs.onHidden) {
                    vnode.attrs.onHidden(e);
                }
            });
        },
        view: function (vnode) {
            let size = "";
            if (vnode.attrs.size) {
                switch (vnode.attrs.size) {
                    case 'sm':
                        size = 'modal-sm';
                        break;
                    case 'lg':
                        size = 'modal-lg';
                        break;
                    case 'xl':
                        size = 'modal-xl';
                        break;
                    default:
                        throw Error("Unknown modal size " + vnode.attrs.size + ". Expected 'sm', 'lg', 'xl' or empty for the default.");

                }
            }
            let vertical = "";
            if (vnode.attrs.vertical) {
                vertical = "modal-dialog-centered";
            }
            return m("div", {
                    id: vnode.attrs.id,
                    class: "modal fade ",
                    tabIndex: -1,
                    role: "dialog"
                },
                m("div", {class: "modal-dialog " + vertical + " " + (size), role: "document"}, [
                    m("div", {class: "modal-content " + vnode.attrs.class}, [
                        m("div", {class: "modal-header"}, [
                            vnode.attrs.header
                        ]),
                        m("div", {class: "modal-body"}, [
                            vnode.attrs.body
                        ]),
                        m("div", {class: "modal-footer"}, [
                            vnode.attrs.footer
                        ])
                    ])
                ])
            );
        }
    }
}

export default Modal;

