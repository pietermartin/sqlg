import {navTo, Route, router} from "./sqlgRoutes";
import {routeTransition} from "meiosis-routing/state";
import meiosis from "meiosis-setup/mergerino";
import stream from "mithril/stream";
import MeiosisRouting from "meiosis-routing";
import merge from 'mergerino';
import TopologyManager from "./topology/TopologyManager";
import Utils from "../utils/utils";

export const ELEMENT_TYPE = Object.freeze({
    SCHEMAS: "Schemas",
    SCHEMA: "Schema",
    VERTEX_LABELS: "VertexLabels",
    VERTEX_LABEL: "VertexLabel",
    EDGE_LABELS: "EdgeLabels",
    EDGE_LABEL: "EdgeLabel"
});

export const PARTITION_TYPE = Object.freeze({
    NONE: "NONE",
    LIST: "LIST",
    RANGE: "RANGE"
});

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
                loggedInUsername: undefined,
                loggedInPassword: undefined,
                userAllowedToEdit: undefined,
                toasts: [],
                jdbcUrl: "",
                username: "",
                selectedTab: "topology",
                editable: false,
                footerNotificationExpanded: false,
                notifications: [],
                treeData: {
                    data: [],
                    refreshData: false,
                    spin: false,
                    selectedTreeItem: undefined,
                    refreshActive: false
                },
                topologyDetails: {
                    elementType: undefined,
                    schema: {
                        name: "",
                        createdOn: "",
                    },
                    schemas: {
                        data: {columns: [], data: []},
                        checkedItems: [],
                        refresh: false,
                        rebuild: false,
                        spin: false,
                        collapsed: true,
                        deletedItems: []
                    },
                    vertexLabels: {
                        data: {columns: [], data: []},
                        checkedItems: [],
                        refresh: false,
                        rebuild: false,
                        spin: false,
                        collapsed: true,
                        deletedItems: []
                    },
                    edgeLabels: {
                        data: {columns: [], data: []},
                        checkedItems: [],
                        refresh: false,
                        rebuild: false,
                        spin: false,
                        collapsed: true,
                        deletedItems: []
                    },
                    abstractLabel: {
                        schemaName: "",
                        label: "VertexLabel",//This is 'VertexLabel' or 'EdgeLabel'
                        name: "",
                        identifierData: {
                            userDefinedIdentifiers: false,
                            identifiers: []
                        },
                        propertyColumns: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false,
                            collapsed: true,
                            deletedItems: []
                        },
                        indexes: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false,
                            collapsed: true,
                            deletedItems: []
                        },
                        inEdgeLabels: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false,
                            collapsed: true,
                            deletedItems: []
                        },
                        outEdgeLabels: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false,
                            collapsed: true,
                            deletedItems: []
                        },
                        partitionType: PARTITION_TYPE.NONE,
                        partitionExpression: "",
                        partitions: {
                            data: {columns: [], data: [], options: {}},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false,
                            collapsed: true,
                            deletedItems: []
                        },
                    }
                }
            }),
        Actions: update => ({
            navigateTo: route => {
                update(navTo(route))
            },
            checkUserAllowedToEdit: () => {
                TopologyManager.checkUserAllowedToEdit((response) => {
                    update({userAllowedToEdit: response.userAllowedToEdit});
                }, (e) => {
                    update({
                        toasts: (toasts) => {
                            toasts.push({
                                id: Utils.uniqueId(),
                                message: e.response.message,
                                autohide: false,
                                type: 'failure'
                            })
                            return toasts;
                        }
                    });
                })
            },
            reset: () => {
                update({
                    loggedInUsername: undefined,
                    loggedInPassword: undefined,
                    userAllowedToEdit: undefined,
                    toasts: [],
                    jdbcUrl: "",
                    username: "",
                    selectedTab: "topology",
                    editable: false,
                    footerNotificationExpanded: false,
                    notifications: [],
                    treeData: {
                        data: [],
                        refreshData: false,
                        spin: false,
                        selectedTreeItem: undefined,
                        refreshActive: false
                    },
                    topologyDetails: {
                        elementType: undefined,
                        schema: {
                            name: "",
                            createdOn: "",
                        },
                        schemas: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false,
                            collapsed: true,
                            deletedItems: []
                        },
                        vertexLabels: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false,
                            collapsed: true,
                            deletedItems: []
                        },
                        edgeLabels: {
                            data: {columns: [], data: []},
                            checkedItems: [],
                            refresh: false,
                            rebuild: false,
                            spin: false,
                            collapsed: true,
                            deletedItems: []
                        },
                        abstractLabel: {
                            schemaName: "",
                            label: "VertexLabel",//This is 'VertexLabel' or 'EdgeLabel'
                            name: "",
                            identifierData: {
                                userDefinedIdentifiers: false,
                                identifiers: []
                            },
                            propertyColumns: {
                                data: {columns: [], data: []},
                                checkedItems: [],
                                refresh: false,
                                rebuild: false,
                                spin: false,
                                collapsed: true,
                                deletedItems: []
                            },
                            indexes: {
                                data: {columns: [], data: []},
                                checkedItems: [],
                                refresh: false,
                                rebuild: false,
                                spin: false,
                                collapsed: true,
                                deletedItems: []
                            },
                            inEdgeLabels: {
                                data: {columns: [], data: []},
                                checkedItems: [],
                                refresh: false,
                                rebuild: false,
                                spin: false,
                                collapsed: true,
                                deletedItems: []
                            },
                            outEdgeLabels: {
                                data: {columns: [], data: []},
                                checkedItems: [],
                                refresh: false,
                                rebuild: false,
                                spin: false,
                                collapsed: true,
                                deletedItems: []
                            },
                            partitionType: PARTITION_TYPE.NONE,
                            partitionExpression: "",
                            partitions: {
                                data: {columns: [], data: [], options: {}},
                                checkedItems: [],
                                refresh: false,
                                rebuild: false,
                                spin: false,
                                collapsed: true,
                                deletedItems: []
                            },
                        }
                    }
                });
            },
            message: (message) => {
                update({
                    toasts: (toasts) => {
                        toasts.push({id: Utils.uniqueId(), message: message.message, type: message.type})
                        return toasts;
                    }
                });
            },
            removeMessage: (item) => {
                update({
                    toasts: (toasts) => {
                        let indexToRemove = -1;
                        for (let i = 0; i < toasts.length; i++) {
                            if (toasts[i].id === item.id) {
                                indexToRemove = i;
                                break;
                            }
                        }
                        if (indexToRemove !== -1) {
                            toasts.splice(indexToRemove, 1);
                        }
                        return toasts;
                    }
                });

            },
            loggedInUsername: (e) => {
                update({loggedInUsername: e.currentTarget.value});
            },
            loggedInPassword: (e) => {
                update({loggedInPassword: e.currentTarget.value});
            },
            login: () => {
                let state = states();
                TopologyManager.login(state.loggedInUsername, state.loggedInPassword, (response) => {
                    actions.retrieveGraphData();
                    actions.retrieveTopologyTree("");
                    update({
                        loggedInPassword: undefined,
                        userAllowedToEdit: response.editable === true,
                        toasts: (toasts) => {
                            toasts.push({
                                id: Utils.uniqueId(),
                                message: "Welcome",
                                type: 'success'
                            })
                            return toasts;
                        }
                    });
                    actions.navigateTo(Route.Sqlg({treeId: "", view: "topology"}))
                }, (e) => {
                    update({
                        toasts: (toasts) => {
                            toasts.push({
                                id: Utils.uniqueId(),
                                message: e.response.message,
                                autohide: false,
                                type: 'failure'
                            })
                            return toasts;
                        }
                    });
                });
            },
            toggleFooterNotification: () => {
                let state = states();
                document.querySelector('#main').style['grid-template-rows'] = `1fr 5px ${state.footerNotificationExpanded ? '20px' : '300px'}`;
                update({
                    footerNotificationExpanded: !state.footerNotificationExpanded
                });
            },
            addNotification: (notification) => {
                update({
                    notifications: (notifications) => {
                        let maxNotifications = 4;
                        if (notifications.length < maxNotifications) {
                            notifications.unshift(notification);
                        } else {
                            notifications.splice(maxNotifications - 1, 1);
                            notifications.unshift(notification);
                        }
                        return notifications;
                    }
                });
            },
            editable: (edit) => {
                update({
                    editable: edit,
                    topologyDetails: {
                        abstractLabel: {
                            propertyColumns: {
                                data: {
                                    columns: (columns) => {
                                        if (edit === false) {
                                            let index = -1;
                                            for (let i = 0; i < columns.length; i++) {
                                                let c = columns[i];
                                                if (c.id === 'deletion') {
                                                    index = i;
                                                }
                                            }
                                            if (index > -1) {
                                                columns.splice(index, 1);
                                            }
                                        }
                                        return columns;
                                    }
                                },
                                refresh: true,
                                rebuild: true
                            }
                        }
                    }
                });
            },
            deleteSchema: () => {
                let state = states();
                TopologyManager.deleteSchema(
                    state.topologyDetails.schema.name,
                    () => {

                    }, () => {

                    });
            },
            deleteAbstractLabel: () => {
                let state = states();
                TopologyManager.deleteAbstractLabel(
                    state.topologyDetails.abstractLabel.schemaName,
                    state.topologyDetails.abstractLabel.name,
                    state.topologyDetails.abstractLabel.label === 'VertexLabel' ? 'vertex' : 'edge',
                    () => {

                    }, () => {

                    });
            },
            deleteSchemas: () => {
                let state = states();
                TopologyManager.deleteSchemas(
                    state.topologyDetails.schemas.deletedItems.map(i => i.name),
                    () => {
                        update({
                            topologyDetails: {
                                schemas: {
                                    deletedItems: (deletedItems) => {
                                        deletedItems.splice(0, deletedItems.length);
                                        return deletedItems;
                                    }
                                }
                            }
                        });
                    }, () => {

                    });
            },
            deleteVertexLabels: () => {
                let state = states();
                TopologyManager.deleteVertexLabels(
                    state.topologyDetails.schema.name,
                    state.topologyDetails.vertexLabels.deletedItems.map(i => i.name),
                    () => {
                        update({
                            topologyDetails: {
                                vertexLabels: {
                                    deletedItems: (deletedItems) => {
                                        deletedItems.splice(0, deletedItems.length);
                                        return deletedItems;
                                    }
                                }
                            },
                            toasts: (toasts) => {
                                toasts.push({id: Utils.uniqueId(), message: "Submitted vertex label deletion"})
                                return toasts;
                            }
                        });
                    }, (e) => {
                        update({
                            toasts: (toasts) => {
                                toasts.push({
                                    id: Utils.uniqueId(),
                                    message: e.message,
                                    autohide: false,
                                    type: 'failure'
                                })
                                return toasts;
                            }
                        });
                    });
            },
            deleteEdgeLabels: () => {
                let state = states();
                TopologyManager.deleteEdgeLabels(
                    state.topologyDetails.schema.name,
                    state.topologyDetails.edgeLabels.deletedItems.map(i => i.name),
                    () => {
                        update({
                            topologyDetails: {
                                edgeLabels: {
                                    deletedItems: (deletedItems) => {
                                        deletedItems.splice(0, deletedItems.length);
                                        return deletedItems;
                                    }
                                }
                            }
                        });
                    }, () => {

                    });
            },
            deleteProperties: () => {
                let state = states();
                TopologyManager.deleteProperties(
                    state.topologyDetails.abstractLabel.schemaName,
                    state.topologyDetails.abstractLabel.name,
                    state.topologyDetails.elementType === ELEMENT_TYPE.VERTEX_LABEL ? "vertex" : "edge",
                    state.topologyDetails.abstractLabel.propertyColumns.deletedItems.map(i => i.name),
                    () => {
                        actions.message({message: "Submitted", type: "info"});
                        update({
                            topologyDetails: {
                                abstractLabel: {
                                    propertyColumns: {
                                        deletedItems: (deletedItems) => {
                                            deletedItems.splice(0, deletedItems.length);
                                            return deletedItems;
                                        }
                                    }
                                }
                            }
                        });
                    }, (e) => {
                        actions.message({message: "Failed to submit properties deletion.", type: "failure"});
                    });
            },
            deleteIndexes: () => {
                let state = states();
                TopologyManager.deleteIndexes(
                    state.topologyDetails.abstractLabel.schemaName,
                    state.topologyDetails.abstractLabel.name,
                    state.topologyDetails.elementType === ELEMENT_TYPE.VERTEX_LABEL ? "vertex" : "edge",
                    state.topologyDetails.abstractLabel.indexes.deletedItems.map(i => i.name),
                    () => {
                        update({
                            topologyDetails: {
                                abstractLabel: {
                                    indexes: {
                                        deletedItems: (deletedItems) => {
                                            deletedItems.splice(0, deletedItems.length);
                                            return deletedItems;
                                        }
                                    }
                                }
                            }
                        });
                    }, () => {

                    });
            },
            deleteInEdgeLabels: () => {
                let state = states();
                TopologyManager.deleteInEdgeLabels(
                    state.topologyDetails.abstractLabel.schemaName,
                    state.topologyDetails.abstractLabel.name,
                    state.topologyDetails.abstractLabel.inEdgeLabels.deletedItems.map(i => i.name),
                    () => {
                        update({
                            topologyDetails: {
                                abstractLabel: {
                                    inEdgeLabels: {
                                        deletedItems: (deletedItems) => {
                                            deletedItems.splice(0, deletedItems.length);
                                            return deletedItems;
                                        }
                                    }
                                }
                            }
                        });
                    }, () => {

                    });
            },
            deleteOutEdgeLabels: () => {
                let state = states();
                TopologyManager.deleteOutEdgeLabels(
                    state.topologyDetails.abstractLabel.schemaName,
                    state.topologyDetails.abstractLabel.name,
                    state.topologyDetails.abstractLabel.outEdgeLabels.deletedItems.map(i => i.name),
                    () => {
                        update({
                            topologyDetails: {
                                abstractLabel: {
                                    inEdgeLabels: {
                                        deletedItems: (deletedItems) => {
                                            deletedItems.splice(0, deletedItems.length);
                                            return deletedItems;
                                        }
                                    }
                                }
                            }
                        });
                    }, () => {

                    });
            },
            deletePartitions: () => {
                let state = states();
                TopologyManager.deletePartitions(
                    state.topologyDetails.abstractLabel.schemaName,
                    state.topologyDetails.abstractLabel.name,
                    state.topologyDetails.elementType === ELEMENT_TYPE.VERTEX_LABEL ? "vertex" : "edge",
                    state.topologyDetails.abstractLabel.partitions.deletedItems.map(i => i.name),
                    () => {
                        update({
                            topologyDetails: {
                                abstractLabel: {
                                    partitions: {
                                        deletedItems: (deletedItems) => {
                                            deletedItems.splice(0, deletedItems.length);
                                            return deletedItems;
                                        }
                                    }
                                }
                            }
                        });
                    }, () => {

                    });
            },
            retrieveGraphData: () => {
                TopologyManager.retrieveGraphData((data) => {
                    update({
                        jdbcUrl: data.jdbcUrl,
                        username: data.username
                    });
                }, (e) => {
                    update({
                        toasts: (toasts) => {
                            toasts.push({id: Utils.uniqueId(), message: e.message, autohide: false})
                            return toasts;
                        }
                    });
                })
            },
            retrieveTopologyTree: (selectedItemId) => {
                update({
                    treeData: {
                        spin: true
                    },
                });
                TopologyManager.retrieveTopologyTree(selectedItemId, function (result) {
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
                        toasts: (toasts) => {
                            toasts.push({id: Utils.uniqueId(), message: e.message, autohide: false})
                            return toasts;
                        }
                    });
                });
            },
            retrieveSchemasFromTree: (item) => {
                if (item !== null) {
                    if (item.indent !== 0) {
                        throw Error("Expected indent === 0");
                    }
                    let state = states();
                    let toUpdate = navTo([Route.Sqlg({
                        treeId: item.id,
                        view: state.selectedTab
                    })]);
                    toUpdate.treeData = {
                        selectedTreeItem: item
                    }
                    toUpdate.topologyDetails = {
                        elementType: ELEMENT_TYPE.SCHEMAS
                    };
                    update(toUpdate);
                    actions.retrieveSchemas();
                }
            },
            retrieveSchemas: () => {
                let toUpdate = {
                    topologyDetails: {
                        elementType: ELEMENT_TYPE.SCHEMAS,
                        schemas: (schemas) => {
                            schemas.data.data.splice(0, schemas.data.data.length);
                            schemas.spin = true;
                            schemas.refresh = true;
                            return schemas;
                        }
                    }
                };
                update(toUpdate);
                TopologyManager.retrieveSchemas(details => {
                    update({
                        topologyDetails: {
                            elementType: ELEMENT_TYPE.SCHEMAS,
                            schemas: (schemas) => {
                                schemas.rebuild = schemas.data.columns !== details.data.columns;
                                schemas.data.columns = details.columns;
                                schemas.data.data.splice(0, schemas.data.data.length);
                                for (let i = 0; i < details.data.length; i++) {
                                    schemas.data.data.push(details.data[i]);
                                }
                                schemas.spin = false;
                                schemas.refresh = true;
                                return schemas;
                            }
                        }
                    })
                }, (e) => {

                });
            },
            retrieveSchemaDetailsFromTree: (item) => {
                if (item !== null) {
                    if (item.indent !== 1) {
                        throw Error("Expected indent === 1");
                    }
                    let state = states();
                    let toUpdate = navTo([Route.Sqlg({
                        treeId: item.id,
                        view: state.selectedTab
                    })]);
                    toUpdate.treeData = {
                        selectedTreeItem: item
                    }
                    toUpdate.topologyDetails = {
                        elementType: ELEMENT_TYPE.SCHEMA
                    }
                    update(toUpdate);
                    actions.retrieveSchemaDetails(item.schemaName);
                }
            },
            retrieveSchemaDetails: (schemaName) => {
                let toUpdate = {
                    topologyDetails: {
                        elementType: ELEMENT_TYPE.SCHEMA,
                        schema: {
                            name: "",
                            createdOn: "",
                        }
                    }
                };
                update(toUpdate);
                TopologyManager.retrieveSchemaDetails(schemaName, details => {
                    update({
                        topologyDetails: {
                            elementType: ELEMENT_TYPE.SCHEMA,
                            schema: {
                                name: details.schema.schemaName,
                                createdOn: details.schema.createdOn
                            }
                        }
                    })
                });
            },
            retrieveVertexLabelsFromTree: (item) => {
                if (item !== null) {
                    if (item.indent !== 2) {
                        throw Error("Expected indent === 2");
                    }
                    let state = states();
                    let toUpdate = navTo([Route.Sqlg({
                        treeId: item.id,
                        view: state.selectedTab
                    })]);
                    toUpdate.treeData = {
                        selectedTreeItem: item
                    }
                    toUpdate.topologyDetails = {
                        elementType: ELEMENT_TYPE.VERTEX_LABELS
                    };
                    update(toUpdate);
                    actions.retrieveVertexLabels(item.parents[1]);
                }
            },
            retrieveVertexLabels: (schemaName) => {
                let toUpdate = {
                    topologyDetails: {
                        elementType: ELEMENT_TYPE.VERTEX_LABELS,
                        schema: {
                            name: schemaName
                        },
                        vertexLabels: (vertexLabels) => {
                            vertexLabels.data.data.splice(0, vertexLabels.data.data.length);
                            vertexLabels.spin = true;
                            vertexLabels.refresh = true;
                            return vertexLabels;
                        }
                    }
                };
                update(toUpdate);
                TopologyManager.retrieveVertexLabels(schemaName, details => {
                    update({
                        topologyDetails: {
                            elementType: ELEMENT_TYPE.VERTEX_LABELS,
                            schema: {
                                name: schemaName
                            },
                            vertexLabels: (vertexLabels) => {
                                vertexLabels.rebuild = vertexLabels.data.columns !== details.data.columns;
                                vertexLabels.data.columns = details.columns;
                                vertexLabels.data.data.splice(0, vertexLabels.data.data.length);
                                for (let i = 0; i < details.data.length; i++) {
                                    vertexLabels.data.data.push(details.data[i]);
                                }
                                vertexLabels.spin = false;
                                vertexLabels.refresh = true;
                                return vertexLabels;
                            }
                        }
                    })
                }, (e) => {

                });
            },
            retrieveEdgeLabelsFromTree: (item) => {
                if (item !== null) {
                    if (item.indent !== 2) {
                        throw Error("Expected indent === 2");
                    }
                    let state = states();
                    let toUpdate = navTo([Route.Sqlg({
                        treeId: item.id,
                        view: state.selectedTab
                    })]);
                    toUpdate.treeData = {
                        selectedTreeItem: item
                    }
                    toUpdate.topologyDetails = {
                        elementType: ELEMENT_TYPE.EDGE_LABELS
                    };
                    update(toUpdate);
                    actions.retrieveEdgeLabels(item.parents[1]);
                }
            },
            retrieveEdgeLabels: (schemaName) => {
                let toUpdate = {
                    topologyDetails: {
                        elementType: ELEMENT_TYPE.EDGE_LABELS,
                        edgeLabels: (edgeLabels) => {
                            edgeLabels.data.data.splice(0, edgeLabels.data.data.length);
                            edgeLabels.spin = true;
                            edgeLabels.refresh = true;
                            return edgeLabels;
                        }
                    }
                };
                update(toUpdate);
                TopologyManager.retrieveEdgeLabels(schemaName, details => {
                    update({
                        topologyDetails: {
                            elementType: ELEMENT_TYPE.EDGE_LABELS,
                            edgeLabels: (edgeLabels) => {
                                edgeLabels.rebuild = edgeLabels.data.columns !== details.data.columns;
                                edgeLabels.data.columns = details.columns;
                                edgeLabels.data.data.splice(0, edgeLabels.data.data.length);
                                for (let i = 0; i < details.data.length; i++) {
                                    edgeLabels.data.data.push(details.data[i]);
                                }
                                edgeLabels.spin = false;
                                edgeLabels.refresh = true;
                                return edgeLabels;
                            }
                        }
                    })
                }, (e) => {

                });
            },
            retrieveAbstractLabelDetailsFromTree: (item) => {
                if (item !== null) {
                    if (item.indent !== 3) {
                        throw Error("Expected indent === 3");
                    }
                    let state = states();
                    let toUpdate = navTo([Route.Sqlg({
                        treeId: item.id,
                        view: state.selectedTab
                    })]);
                    toUpdate.treeData = {
                        selectedTreeItem: item
                    }
                    update(toUpdate);
                    actions.retrieveAbstractLabelDetails(item.schemaName, item.abstractLabel, item.vertexOrEdge);
                }
            },
            retrieveAbstractLabelDetails: (schemaName, abstractLabel, vertexOrEdge) => {
                let toUpdate = {
                    topologyDetails: {
                        elementType: vertexOrEdge === 'vertex' ? ELEMENT_TYPE.VERTEX_LABEL : ELEMENT_TYPE.EDGE_LABEL,
                        abstractLabel: {
                            // schemaName: "",
                            // label: "",
                            // name: "",
                            identifierData: (identifierData) => {
                                identifierData.identifiers.splice(0, identifierData.identifiers.length);
                                return identifierData;
                            },
                            propertyColumns: (propertyColumns) => {
                                propertyColumns.data.data.splice(0, propertyColumns.data.data.length);
                                propertyColumns.spin = true;
                                propertyColumns.refresh = true;
                                return propertyColumns;
                            },
                            indexes: (indexes) => {
                                indexes.data.data.splice(0, indexes.data.data.length);
                                indexes.spin = true;
                                indexes.refresh = true;
                                return indexes;
                            },
                            inEdgeLabels: (inEdgeLabels) => {
                                inEdgeLabels.data.data.splice(0, inEdgeLabels.data.data.length);
                                inEdgeLabels.spin = true;
                                inEdgeLabels.refresh = true;
                                return inEdgeLabels;
                            },
                            outEdgeLabels: (outEdgeLabels) => {
                                outEdgeLabels.data.data.splice(0, outEdgeLabels.data.data.length);
                                outEdgeLabels.spin = true;
                                outEdgeLabels.refresh = true;
                                return outEdgeLabels;
                            },
                            partitionType: PARTITION_TYPE.NONE,
                            partitionExpression: "",
                            partitions: (partitions) => {
                                partitions.data.data.splice(0, partitions.data.data.length);
                                partitions.spin = true;
                                partitions.refresh = true;
                                partitions.options = undefined;
                                return partitions;
                            }
                        }
                    }
                };
                update(toUpdate);
                TopologyManager.retrieveAbstractLabelDetails(
                    schemaName,
                    abstractLabel,
                    vertexOrEdge,
                    details => {
                        if (details.abstractLabel.label !== ELEMENT_TYPE.VERTEX_LABEL &&
                            details.abstractLabel.label !== ELEMENT_TYPE.EDGE_LABEL
                        ) {
                            throw new Error("Unknown type returned from the server. " + details.type);
                        }
                        update({
                            topologyDetails: {
                                elementType: details.abstractLabel.label,
                                schema: undefined,
                                abstractLabel: {
                                    schemaName: details.abstractLabel.schemaName,
                                    label: details.abstractLabel.label,
                                    name: details.abstractLabel.name,
                                    identifierData: (identifierData) => {
                                        identifierData.userDefinedIdentifiers = details.abstractLabel.identifierData.userDefinedIdentifiers;
                                        identifierData.identifiers.splice(0, identifierData.identifiers.length);
                                        for (let i = 0; i < details.abstractLabel.identifierData.identifiers.length; i++) {
                                            identifierData.identifiers.push(details.abstractLabel.identifierData.identifiers[i]);
                                        }
                                        return identifierData;
                                    },
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
                                    },
                                    partitionType: details.abstractLabel.partitionType,
                                    partitionExpression: details.abstractLabel.partitionExpression,
                                    partitions: (partitions) => {
                                        partitions.rebuild = partitions.data.columns !== details.abstractLabel.partitions.columns;
                                        partitions.data.columns = details.abstractLabel.partitions.columns;
                                        partitions.data.options = details.abstractLabel.partitions.options;
                                        partitions.data.data.splice(0, partitions.data.data.length);
                                        for (let i = 0; i < details.abstractLabel.partitions.data.length; i++) {
                                            partitions.data.data.push(details.abstractLabel.partitions.data[i]);
                                        }
                                        partitions.spin = false;
                                        partitions.refresh = true;
                                        return partitions;
                                    }
                                },
                            }
                        });
                        if (details.abstractLabel.label === 'VertexLabel') {
                            update({
                                topologyDetails: {
                                    abstractLabel: {
                                        inEdgeLabels: (inEdgeLabels) => {
                                            inEdgeLabels.rebuild = inEdgeLabels.data.columns !== details.abstractLabel.inEdgeLabels.columns;
                                            inEdgeLabels.data.columns = details.abstractLabel.inEdgeLabels.columns;
                                            inEdgeLabels.data.data.splice(0, inEdgeLabels.data.data.length);
                                            for (let i = 0; i < details.abstractLabel.inEdgeLabels.data.length; i++) {
                                                inEdgeLabels.data.data.push(details.abstractLabel.inEdgeLabels.data[i]);
                                            }
                                            inEdgeLabels.spin = false;
                                            inEdgeLabels.refresh = true;
                                            return inEdgeLabels;
                                        },
                                        outEdgeLabels: (outEdgeLabels) => {
                                            outEdgeLabels.rebuild = outEdgeLabels.data.columns !== details.abstractLabel.outEdgeLabels.columns;
                                            outEdgeLabels.data.columns = details.abstractLabel.outEdgeLabels.columns;
                                            outEdgeLabels.data.data.splice(0, outEdgeLabels.data.data.length);
                                            for (let i = 0; i < details.abstractLabel.outEdgeLabels.data.length; i++) {
                                                outEdgeLabels.data.data.push(details.abstractLabel.outEdgeLabels.data[i]);
                                            }
                                            outEdgeLabels.spin = false;
                                            outEdgeLabels.refresh = true;
                                            return outEdgeLabels;
                                        }
                                    },
                                }
                            });

                        } else {

                        }
                    }, e => {
                        console.log(e);
                    });
            },
            setTreeRefresh: refresh => {
                update({
                    treeData: {
                        refreshData: refresh
                    }
                });
            },
            setPropertiesGridRefresh: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            propertyColumns: {
                                refresh: true,
                                rebuild: false
                            }
                        }
                    }
                });
            },
            setPropertiesGridExpand: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            propertyColumns: {
                                refresh: true,
                                rebuild: true,
                                collapsed: false
                            }
                        }
                    }
                });
            },
            setPropertiesGridCollapse: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            propertyColumns: {
                                collapsed: true
                            }
                        }
                    }
                });
            },
            setIndexesGridExpand: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            indexes: {
                                refresh: true,
                                rebuild: true,
                                collapsed: false
                            }
                        }
                    }
                });
            },
            setIndexesGridCollapse: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            indexes: {
                                collapsed: true
                            }
                        }
                    }
                });
            },
            setInEdgeLabelsGridExpand: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            inEdgeLabels: {
                                refresh: true,
                                rebuild: true,
                                collapsed: false
                            }
                        }
                    }
                });
            },
            setInEdgeLabelsGridCollapse: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            inEdgeLabels: {
                                collapsed: true
                            }
                        }
                    }
                });
            },
            setOutEdgeLabelsGridExpand: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            outEdgeLabels: {
                                refresh: true,
                                rebuild: true,
                                collapsed: false
                            }
                        }
                    }
                });
            },
            setOutEdgeLabelsGridCollapse: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            outEdgeLabels: {
                                collapsed: true
                            }
                        }
                    }
                });
            },
            setPartitionsGridExpand: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            partitions: {
                                refresh: true,
                                rebuild: true,
                                collapsed: false
                            }
                        }
                    }
                });
            },
            setPartitionsGridCollapse: () => {
                update({
                    topologyDetails: {
                        abstractLabel: {
                            partitions: {
                                collapsed: true
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