import m from 'mithril';

function RegularLink() {

    let enabled = false;
    let isButton = true;

    return {
        view: function (vnode) {
            if (vnode.attrs.href === undefined) {
                throw Error("A regular link button must have a href attr!");
            }
            enabled = vnode.attrs.enabled === undefined || vnode.attrs.enabled;
            let attrs = {
                ...vnode.attrs.attrs,
                href: vnode.attrs.href,
                title: vnode.attrs.text,
                class: `${'btn btn-sm ' + (enabled ? 'enabled ' : 'disabled ') + (vnode.attrs.class ? vnode.attrs.class : '')}`,
                onclick: enabled ? vnode.attrs.onclick : function () {
                    /*noop*/
                }
            };
            if (vnode.attrs.id !== undefined) {
                attrs.id = vnode.attrs.id;
            }
            return m("a",
                attrs, vnode.attrs.text ? [
                        m('i', {class: "fas " + (vnode.attrs.icon ? vnode.attrs.icon : '')}),
                        m("span", " " + vnode.attrs.text)
                    ] :
                    m('i', {class: "fas " + (vnode.attrs.icon ? vnode.attrs.icon : '')})
            );
        }
    }
}

export default RegularLink;
