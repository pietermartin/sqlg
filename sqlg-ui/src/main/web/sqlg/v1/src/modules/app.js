import m from "mithril";
import Stream from "mithril/stream";
import mergerino from 'mergerino';
import MainLayout from "./mainLayout";
import LeftPane from "./leftPane";
import RightPane from "./rightPane";
import TopologyManager from "./topology/TopologyManager";

const accumulator = mergerino;
const [stream, scan] = [Stream, Stream.scan];

function App(ignore) {

    const dataService = state => {
    };

    const dataEffect = actions => state => {
    };

    let component = {
        data: {
            topologyDetails: {
                type: "",
                schema: {
                    label: "",
                    name: "",
                    ID: "",
                    createdOn: "",
                },
                vertexLabel: {
                    label: "",
                    name: "",
                    ID: "",
                    createdOn: "",
                },
                edgeLabel: {
                    label: "",
                    name: "",
                    ID: "",
                    createdOn: "",
                }
                
            }
        },
        actions: update => ({
            retrieveSchemaDetails: (item) => {
                TopologyManager.retrieveSchemaDetails(item, details => {
                    if (details.schema !== undefined) {
                        details.type = "Schema";
                    } else if (details.vertexLabel !== undefined) {
                        details.type = "VertexLabel";
                    } else {
                        throw new Error("Unknown type returned from the server. " + details.type);
                    }
                    update({
                        topologyDetails: details
                    });
                }, e => {

                });
            }
        }),
        services: [dataService],
        effects: (actions) => [dataEffect(actions)]
    };

    let update = stream();
    const runServices = startingState =>
        component.services.reduce(
            (state, service) => accumulator(state, service(state)),
            startingState
        );
    const states = scan(
        (state, patch) => runServices(accumulator(state, patch)),
        runServices(component.data),
        update
    );
    const actions = component.actions(update);
    const effects = component.effects(actions);
    states.map(state =>
        effects.forEach(effect => effect(state))
    );

    return {
        view: function (vnode) {
            return m(MainLayout, {
                width: '25%',
                leftPane: m(LeftPane, {state: states(), actions}),
                rightPane: m(RightPane, {state: states(), actions})
            });
        }
    }

}

export default App;