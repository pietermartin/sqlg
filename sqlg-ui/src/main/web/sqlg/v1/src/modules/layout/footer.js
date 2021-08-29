import m from "mithril";

function Footer(ignore) {

    return {
        oninit: ({attrs: {state, actions}}) => {
            $.Topic('/sqlg-ui/notification').subscribe((message) => {
                actions.addNotification(message['message']);
                m.redraw();
            });
        },
        view: ({attrs: {state, actions}}) => {
            return m("div#main-footer", [
                m("div.footer-notification.bg-dark.text-info.ps-1", state.notifications.map((item) => {
                    return [m("small", item), m("br")];
                })),
                m("div.footer-bottom.bg-light", m("a", {
                    href: "#none",
                    text: "",
                    onclick: (e) => {
                        actions.toggleFooterNotification();
                        e.preventDefault();
                        e.stopPropagation();
                    }
                }, [
                    m('i', {class: "fas fa-bell"})
                ]))
            ])
        }
    }

}

export default Footer;