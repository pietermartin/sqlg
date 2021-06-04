import m from "mithril";
import CmMithrilGlobal from "../../utils/CmMithrilGlobal";
import ToastDetail from "./toastDetail";
import Utils from "../../utils/utils";

const Toast = {

    "primary": "primary",
    "secondary": "secondary",
    "success": "success",
    "failure": "failure",
    "warning": "warning",
    "info": "info",
    "light": "light",
    "dark": "dark",
    "white": "white",
    "transparent": "transparent",

    remove: function (id) {
        let index = -1;
        for (let i = 0; i < CmMithrilGlobal.toasts.length; i++) {
            let t = CmMithrilGlobal.toasts[i];
            if (t.id === id) {
                index = i;
                break;
            }
        }
        if (index > -1) {
            CmMithrilGlobal.toasts.splice(index, 1);
        }
    },

    view: function (vnode) {
        return m("div", {style: "position: absolute; top: 2rem; right: 2rem; z-index: 10000"}, [
            CmMithrilGlobal.toasts.map(function (item) {
                if (item.id === undefined) {
                    item.id = Utils.uniqueId();
                }
                return m(ToastDetail, {key: item.id, item: item, remove: Toast.remove});
            })
        ]);
    }
};

export default Toast;
