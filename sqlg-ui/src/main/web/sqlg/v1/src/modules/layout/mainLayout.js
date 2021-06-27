import m from "mithril";
import "../../components/jquery-splitter/jquery.splitter"
import "../../components/jquery-splitter/jquery.splitter.css"

function MainLayout(ignore) {

    return {
        oncreate: ({attrs: {width}}) => {
            let main = $('#main');
            let splitter = main.split({
                orientation: 'vertical',
                limit: 10,
                position: width,
                onDrag: function(event) {
                    console.log(splitter.position());
                }
            });
            main.data("splitter", splitter);
        },
        view: (vnode) => {
            return m("div#main", [
                m("div#left-pane", vnode.attrs.leftPane),
                m("div#right-pane", vnode.attrs.rightPane)
            ]);
        }
    }

}

export default MainLayout;