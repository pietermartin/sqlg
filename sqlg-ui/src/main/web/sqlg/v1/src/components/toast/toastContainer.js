import m from "mithril";
import Toast from "./toastDetail";

function ToastContainer(ignore) {

    return {
        oncreate: ({attrs: {state, actions}}) => {
        },
        view: ({attrs: {state, actions}}) => {
            return m("div.toast-container.position-fixed.top-0.end-0.p-3", state.toasts.filter(item => item.id !== undefined).map(function (item) {
                    return m(Toast, {item: item, actions: actions})
                })
            )
        }
    }

}

export default ToastContainer;