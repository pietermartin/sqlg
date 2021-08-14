import {navTo, Route, router} from "./sqlgRoutes";
import {routeTransition} from "meiosis-routing/state";
import meiosis from "meiosis-setup/mergerino";
import stream from "mithril/stream";
import MeiosisRouting from "meiosis-routing";
import merge from 'mergerino';
import TopologyManager from "./topology/TopologyManager";

function SqlgModel() {

    let SCREEN_NAME = "Sqlg";
    const {Routing} = MeiosisRouting.state;

    const treeSetSelectionEffect = (update, actions) => state => {
        let {arrive, leave} = state.routeTransition;
        if (arrive.Sqlg !== undefined && arrive.Sqlg.params.treeId !== undefined && arrive.Sqlg.params.treeId !== "" && state.treeData.data.length > 0) {
            let selectedItem;
            let selectedItemId = arrive.Sqlg.params.treeId;
            for (let i = 0; i < state.treeData.data.length; i++) {
                if (state.treeData.data[i].id === selectedItemId) {
                    selectedItem = state.treeData.data[i];
                    break;
                }
            }
            if (selectedItem.id !== state.treeData.selectedTreeItem.id) {
                update({
                    treeData: {
                        selectedTreeItem: selectedItem,
                        refreshData: true
                    },
                });
            }
        }
        if (arrive.Sqlg !== undefined && arrive.Sqlg.params.view !== state.selectedTab) {
            //Need to update the state from the url
            update({selectedTab: arrive.Sqlg.params.view});
        }
    };

    const routeService = state => ({
        routeTransition: () => routeTransition(state.route, state.nextRoute),
        route: state.nextRoute
    });

    const filterInitializeEffect = (update, actions) => state => {
    };

    let app = {
        initial: Object.assign(
            navTo([Route.Sqlg({treeId: "", view: "topology"})]),
            {
                messages: [],
                jdbcUrl: "",
                username: "",
                selectedTab: "topology",
                treeData: {
                    data: [],
                    refreshData: false,
                    spin: false,
                    selectedTreeItem: undefined,
                    refreshActive: false
                },
                topologyDetails: {
                    type: "",
                    schema: {
                        label: "",
                        name: "",
                        ID: "",
                        createdOn: "",
                    },
                    abstractLabel: {
                        propertyColumns: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false
                        },
                        indexes: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false
                        }
                    }
                }
            }),
        Actions: update => ({
            navigateTo: route => {
                update(navTo(route))
            },
            retrieveGraphData: () => {
                TopologyManager.retrieveGraphData((data) => {
                    update({
                        jdbcUrl: data.jdbcUrl,
                        username: data.username
                    });
                }, (e) => {
                    update({
                        messages: () => {
                            return [{
                                type: "failure",
                                message: e.message,
                                autohide: false
                            }]
                        }
                    });
                })
            },
            retrieveSchema: (selectedItemId) => {
                update({
                    treeData: {
                        spin: true
                    },
                });
                TopologyManager.retrieveSchema(selectedItemId, function (result) {
                    let selectedItem;
                    for (let i = 0; i < result.length; i++) {
                        if (result[i].id === selectedItemId) {
                            selectedItem = result[i];
                            break;
                        }
                    }
                    update({
                        treeData: {
                            data: result,
                            selectedTreeItem: selectedItem,
                            refreshData: true,
                            refreshActive: false,
                            spin: false
                        },
                    });
                }, function (e) {
                    update({
                        messages: () => {
                            return [{
                                type: "failure",
                                message: e.message,
                                autohide: false
                            }]
                        }
                    });
                });
            },
            retrieveSchemaDetails: (item) => {
                if (item !== null) {
                    let state = states();
                    let toUpdate = navTo([Route.Sqlg({
                        treeId: item.id,
                        view: state.selectedTab
                    })]);
                    toUpdate.treeData = {
                        selectedTreeItem: item
                    }
                    toUpdate.topologyDetails = {
                        abstractLabel: {
                            propertyColumns: (propertyColumns) => {
                                propertyColumns.data.data.splice(0, propertyColumns.data.data.length);
                                propertyColumns.spin = (item.indent === 3);
                                propertyColumns.refresh = true;
                                return propertyColumns;
                            },
                            indexes: (indexes) => {
                                indexes.data.data.splice(0, indexes.data.data.length);
                                indexes.spin = (item.indent === 3);
                                indexes.refresh = true;
                                return indexes;
                            }
                        }
                    }
                    update(toUpdate);
                    if (item.indent === 3) {
                        TopologyManager.retrieveSchemaDetails(item, details => {
                            if (details.schema !== undefined) {
                                details.type = "Schema";
                            } else if (details.abstractLabel !== undefined) {
                                details.type = "AbstractLabel";
                            } else {
                                throw new Error("Unknown type returned from the server. " + details.type);
                            }
                            update({
                                topologyDetails: {
                                    type: "AbstractLabel",
                                    abstractLabel: {
                                        label: details.abstractLabel.label,
                                        name: details.abstractLabel.name,
                                        identifierData: details.abstractLabel.identifierData,
                                        propertyColumns: (propertyColumns) => {
                                            propertyColumns.rebuild = propertyColumns.data.columns !== details.abstractLabel.propertyColumns.columns;
                                            propertyColumns.data.columns = details.abstractLabel.propertyColumns.columns;
                                            propertyColumns.data.data.splice(0, propertyColumns.data.data.length);
                                            for (let i = 0; i < details.abstractLabel.propertyColumns.data.length; i++) {
                                                propertyColumns.data.data.push(details.abstractLabel.propertyColumns.data[i]);
                                            }
                                            propertyColumns.spin = false;
                                            propertyColumns.refresh = true;
                                            return propertyColumns;
                                        },
                                        indexes: (indexes) => {
                                            indexes.rebuild = indexes.data.columns !== details.abstractLabel.indexes.columns;
                                            indexes.data.columns = details.abstractLabel.indexes.columns;
                                            indexes.data.data.splice(0, indexes.data.data.length);
                                            for (let i = 0; i < details.abstractLabel.indexes.data.length; i++) {
                                                indexes.data.data.push(details.abstractLabel.indexes.data[i]);
                                            }
                                            indexes.spin = false;
                                            indexes.refresh = true;
                                            return indexes;
                                        }
                                    },
                                }
                            });
                        }, e => {
                            console.log(e);
                        });
                    }
                }
            },
            setTreeRefresh: refresh => {
                update({
                    treeData: {
                        refreshData: refresh
                    }
                });
            },
            setPropertiesGridRefresh: refresh => {
                update({
                    topologyDetails:  {
                        abstractLabel: {
                            propertyColumns: {
                                refresh: refresh
                            }
                        }
                    }
                });
            },
            setActiveTab: tab => {
                let state = states();
                let toUpdate = navTo([Route.Sqlg({
                    treeId: state.treeData.selectedTreeItem !== undefined ? state.treeData.selectedTreeItem.id : "",
                    view: tab
                })]);
                toUpdate.selectedTab = tab;
                update(toUpdate);
            }
        }),
        services: [routeService],
        Effects: (update, actions) => [filterInitializeEffect(update, actions), treeSetSelectionEffect(update, actions)]
    };

    let {states, actions} = meiosis({stream: stream, merge, app});
    states.map(state => {
            router.locationBarSync(state.route);
        }
    );
    return {states, actions}
}

export default SqlgModel;