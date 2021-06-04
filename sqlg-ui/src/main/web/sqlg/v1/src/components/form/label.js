import m from 'mithril';

function Label(initialNode) {

    return {
        view: function (vnode) {
            if (!vnode.attrs.for) {
                throw Error("Label component must have an 'for' attribute. None found!");
            }
            let attributes = {for: vnode.attrs.for};
            if (vnode.attrs.class) {
                attributes["class"] = vnode.attrs.class;
            }
            if (vnode.attrs.key) {
                attributes["key"] = vnode.attrs.key;
            }
            if (vnode.children.length > 0) {
                return m("label", attributes, vnode.attrs.text, vnode.children);
            } else {
                return m("label", attributes, vnode.attrs.text, m.trust("&nbsp;"));
            }
        }
    }
}

export default Label;
