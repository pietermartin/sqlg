import m from "mithril";

import {Toast} from "bootstrap";

function ToastDetail(ignore) {

    return {
        oncreate: ({attrs: {item}}) => {
            let toastLiveExample = document.getElementById(item.id)
            let toast = new Toast(toastLiveExample)
            toast.show()
        },
        view: ({attrs: {item}}) => {
            return m("div.toast", {id: item.id, "data-bs-autohide": `${item.autohide === undefined ? "true" : item.autohide}`}, [
                m("div.toast-header", [
                    m("strong.me-auto", 'Sqlg'),
                    m("button.btn-close", {type: 'button', "data-bs-dismiss": 'toast'})
                ]),
                m("div.toast-body", item.message)
            ])
        }
    }

}

export default ToastDetail;