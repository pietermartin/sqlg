import m from "mithril";

import {Toast} from "bootstrap";
import Utils from "../../utils/utils";

function ToastDetail(ignore) {

    return {
        oncreate: ({attrs: {item, actions}}) => {
            let toastLiveExample = document.getElementById(item.id)
            let toast = new Toast(toastLiveExample)
            toast.show();
            toastLiveExample.addEventListener('hidden.bs.toast', function () {
                actions.removeMessage(item);
            });
        },
        // id: Utils.uniqueId(), message: e.message, autohide
        view: ({attrs: {item}}) => {
            const {id, message, autohide, type} = item;
            let background;
            if (type === undefined) {
                background = 'bg-primary';
            } else if (type === 'info') {
                background = 'bg-info';
            } else if (type === 'success') {
                background = 'bg-success';
            } else if (type === 'failure') {
                background = 'bg-danger';
            }
            return m("div.toast", {
                id: id,
                "data-bs-autohide": `${autohide === undefined ? true : autohide}`,
                class: background
            }, [
                m("div.toast-header", [
                    m("strong.me-auto", 'Sqlg'),
                    m("button.btn-close", {type: 'button', "data-bs-dismiss": 'toast'})
                ]),
                m("div.toast-body", message)
            ])
        }
    }

}

export default ToastDetail;