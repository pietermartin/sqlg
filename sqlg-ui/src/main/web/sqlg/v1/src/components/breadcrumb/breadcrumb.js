import m from 'mithril';

function BreadCrumb() {

    return {
        view: function (vnode) {
            return m("nav", {"aria-label": "breadcrumb"},
                m("ol.breadcrumb p-1", [
                    vnode.attrs.links.map(function (link) {
                        if (link.active) {
                            return m("li.breadcrumb-item", {
                                class: "active",
                                "aria-current": "page"
                            }, m(m.route.Link, {href: link.href}, link.name));
                        } else {
                            return m("li.breadcrumb-item", m(m.route.Link, {href: link.href}, link.name));
                        }
                    }),
                    vnode.attrs.doc ? vnode.attrs.doc : ""
                ])
            )
        }
    }
}

export default BreadCrumb;
