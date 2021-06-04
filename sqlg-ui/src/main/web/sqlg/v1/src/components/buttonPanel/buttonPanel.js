import m from 'mithril';

function ButtonPanel() {

    return {

        view: function (vnode) {
            let klass = "button-panel bg-light";
            if (vnode.attrs.justify === 'right') {
                klass = klass + " button-panel-right";

            } else if (vnode.attrs.justify === 'left') {
                klass = klass + " button-panel-left";

            } else if (vnode.attrs.justify === 'center') {
                klass = klass + " button-panel-center";

            } else if (vnode.attrs.justify === 'space-between') {
                klass = klass + " button-panel-space-between";
            }
            if (vnode.attrs.class) {
                klass += " " + vnode.attrs.class;
            }
            return m("div", {class: klass}, vnode.children);
        }
    }
}

export default ButtonPanel;
