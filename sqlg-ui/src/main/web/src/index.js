import m from "mithril";
import $ from "jquery";
import 'split-pane/split-pane';
import "split-pane/split-pane.css"
import "./sqlg-ui.scss";

m.mount(document.body, {
    oncreate: () => {
        $('div.split-pane').splitPane();
    },
    view: () => m("div.pretty-split-pane-frame", [
        m("div.split-pane.fixed-left", [
            m("div.split-pane-component", {id: "left-component"}),
            m("div.split-pane-divider", {id: "my-divider"}),
            m("div.split-pane-component", {id: "right-component"})
        ])
    ])
});
