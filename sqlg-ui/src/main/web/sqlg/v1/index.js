import m from "mithril";
import $ from "jquery";
import 'split-pane/split-pane';
import "split-pane/split-pane.css"
import "./sqlg.scss";
import mergerino from "mergerino";
import Stream from "mithril/stream";
import SlickTree from "./components/tree/slicktree";

const [stream, scan] = [Stream, Stream.scan];
const accumulator = mergerino;

const service1 = state => {
    console.log("service1");
};

const DataEffect = actions => state => {
    console.log("DataEffect");
};

let conditions = {
    initial: {
        inc: 1
    },
    Actions: function (update) {
        return {
            togglePrecipitations: function (id) {
                update({
                    [id]: {inc: x => x + 1}
                });
            }
        };
    }
};


let LeftPane = {
    view: ({attrs: {id, state, actions}}) =>
        m("div", "what " + state[id].inc, m(
            "button", {
                onclick: (e) => {
                    actions.togglePrecipitations(id);
                    console.log("halo");
                    // e.redraw = false;
                }
            },
            "Change Units"
        ))
};

let app = {
    initial: {
        leftPane1: conditions.initial,
        leftPane2: conditions.initial
    },
    Actions: function (update) {
        return Object.assign(
            {},
            conditions.Actions(update)
        );
    },

    services: [service1],

    Effects: (_update, actions) => [DataEffect(actions)]
};

const update = stream();

const runServices = startingState =>
    app.services.reduce(
        (state, service) => accumulator(state, service(state)),
        startingState
    );

const states = scan(
    (state, patch) => runServices(accumulator(state, patch)),
    runServices(app.initial),
    update
);

const actions = app.Actions(update, states);
const effects = app.Effects(update, actions);

states.map(state =>
    effects.forEach(effect => effect(state))
);

let App = {
    oncreate: () => {
        $('div.split-pane').splitPane();
        let data = [];
        for (var i = 0; i < 500; i++) {
            data[i] = {
                id: "Task " + i,
                title: "Task " + i,
            };
        }
        const slickTree = new SlickTree({root: 'slickTree1', data: data})
    },
    view: ({attrs: {state, actions}}) => m("div.pretty-split-pane-frame", [
        m("div.split-pane.fixed-left", [
            m("div.split-pane-component", {id: "left-component"}, [
                // m(LeftPane, {id: "leftPane1", state, actions}),
                m("div.slick-tree-container", {id: "slickTree1"})
            ]),
            m("div.split-pane-divider", {id: "my-divider"}),
            m("div.split-pane-component", {id: "right-component"})
        ])
    ])
};

m.mount(document.body, {
    view: () => {
        return m(App, {state: states(), actions})
    }
});

states.map(() => {
    m.redraw();
});