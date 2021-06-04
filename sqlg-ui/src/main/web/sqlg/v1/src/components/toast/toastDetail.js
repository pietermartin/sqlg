import m from "mithril";
import $ from "jquery";
import _ from "underscore";

const ToastDetail = {

    type: function (type) {
        let klass = {class: 'primary', icon: 'fa-times'};
        if (type === 'primary') {
            klass.class = 'primary';
            klass.icon = 'fa-info';
        } else if (type === 'secondary') {
            klass.class = 'secondary';
            klass.icon = 'fa-info';
        } else if (type === 'success') {
            klass.class = 'success';
            klass.icon = 'fa-check';
        } else if (type === 'danger' || type === 'failure') {
            klass.class = 'danger';
            klass.icon = 'fa-exclamation-triangle';
        } else if (type === 'warning') {
            klass.class = 'warning';
            klass.icon = 'fa-exclamation';
        } else if (type === 'info') {
            klass.class = 'info';
            klass.icon = 'fa-info';
        } else if (type === 'light') {
            klass.class = 'light';
            klass.icon = 'fa-info';
        } else if (type === 'dark') {
            klass.class = 'dark';
            klass.icon = 'fa-info';
        } else if (type === 'white') {
            klass.class = 'white';
            klass.icon = 'fa-info';
        } else if (type === 'transparent') {
            klass.class = 'transparent';
            klass.icon = 'fa-info';
        } else if (type === '') {
        } else {
            throw Error('Unknown toast type ' + type);
        }
        return klass;
    },

    onremove: function() {
    },

    oncreate: function (vnode) {
        let item = vnode.attrs.item;
        let t = $(`#${item.id}`);
        t.toast({delay: !!item.delay ? item.delay : 3000, autohide: item.autohide !== undefined ? item.autohide : true});
        t.toast('show');
        t.on('hidden.bs.toast', function (jqueryItem) {
            let id = $(jqueryItem.target).attr("id");
            vnode.attrs.remove(id);
        });
    },

    view: function (vnode) {
        let item = vnode.attrs.item;
        let type = ToastDetail.type(item.type);

        if (item.header) {
            return m("div", {
                id: item.id,
                role: 'alert',
                "aria-live": 'assertive',
                "aria-atomic": "true",
                class: "toast bg-" + type.class + (item.class ? " " + item.class : "")
            }, [
                m("div.toast-header", [
                    m("strong.mr-auto", m("i", {class: "text-" + type.class + " fas " + type.icon}), " " + item.header),
                    m("button.ml-2.mb-1.close", {
                        "data-dismiss": "toast",
                        "aria-label": "Close"
                    }, m("i", {class: "fas fa-times"}))
                ]),
                m("div.toast-body", Array.isArray(item.message) ?
                    _.map(item.message, function (item) {
                        return m("p", m.trust(item.replace(/\n/g, '</br>')));
                    }) :
                    m("p", m.trust(item.message.replace(/\n/g, '</br>'))))
            ]);
        } else {
            return m("div", {
                id: item.id,
                role: 'alert',
                "aria-live": 'assertive',
                "aria-atomic": "true",
                class: "toast bg-" + type.class + (item.class ? " " + item.class : "")
            }, [
                m("div.cm-toast", m("button.ml-2.mr-2.mb-1.close", {
                    "data-dismiss": "toast",
                    "aria-label": "Close"
                }, m("i", {class: "fas fa-times"}))),
                m("div.toast-body.mr-4", m.trust(item.message.replace(/\n/g, '</br>')))
            ]);
        }

    }
};

export default ToastDetail;