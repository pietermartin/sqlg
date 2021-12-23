import m from 'mithril';
import SqlgGlobal from "../SqlgGlobal";

const TopologyManager = {

    login: (username, password, callBack, callBackError) => {
        m.request({
            method: "POST",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "login",
            body : {
                username: username,
                password: password
            }
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    checkUserAllowedToEdit: (callBack, callBackError) => {
        m.request({
            method: "GET",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "/userAllowedToEdit"
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })

    },
    deleteSchema: (schemaName, abstractLabelName, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema/:schemaName",
            params: {
                schemaName: schemaName
            }
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deleteAbstractLabel: (schemaName, abstractLabelName, vertexOrEdge, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema/:schemaName/:abstractLabel/:vertexOrEdge",
            params: {
                schemaName: schemaName,
                abstractLabel: abstractLabelName,
                vertexOrEdge: vertexOrEdge
            }
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deleteSchemas: (schemas, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema",
            body: schemas
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deleteVertexLabels: (schemaName, vertexLabels, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema/:schemaName/vertexLabels",
            params: {
                schemaName: schemaName
            },
            body: vertexLabels
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deleteEdgeLabels: (schemaName, edgeLabels, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema/:schemaName/edgeLabels",
            params: {
                schemaName: schemaName
            },
            body: edgeLabels
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deleteProperties: (schemaName, abstractLabelName, vertexOrEdge, properties, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema/:schemaName/:abstractLabel/:vertexOrEdge/properties",
            params: {
                schemaName: schemaName,
                abstractLabel: abstractLabelName,
                vertexOrEdge: vertexOrEdge,
            },
            body: properties
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deleteIndexes: (schemaName, abstractLabelName, vertexOrEdge, indexes, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema/:schemaName/:abstractLabel/:vertexOrEdge/indexes",
            params: {
                schemaName: schemaName,
                abstractLabel: abstractLabelName,
                vertexOrEdge: vertexOrEdge,
            },
            body: indexes
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deleteInEdgeLabels: (schemaName, abstractLabelName, inEdgeLabels, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema/:schemaName/:abstractLabel/vertex/inEdgeLabels",
            params: {
                schemaName: schemaName,
                abstractLabel: abstractLabelName
            },
            body: inEdgeLabels
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deleteOutEdgeLabels: (schemaName, abstractLabelName, outEdgeLabels, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "delete/schema/:schemaName/:abstractLabel/vertex/outEdgeLabels",
            params: {
                schemaName: schemaName,
                abstractLabel: abstractLabelName
            },
            body: outEdgeLabels
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    deletePartitions: (schemaName, abstractLabelName, vertexOrEdge, partitions, callBack, callBackError) => {
        m.request({
            method: "DELETE",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "schema/:schemaName/:abstractLabel/:vertexOrEdge/partitions",
            params: {
                schemaName: schemaName,
                abstractLabel: abstractLabelName,
                vertexOrEdge: vertexOrEdge,
            },
            body: partitions
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    retrieveGraphData: (callBack, callBackError) => {
        m.request({
            method: "GET",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "graph"
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    retrieveTopologyTree: (selectedItemId, callBack, callBackError) => {
        m.request({
            method: "GET",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "topologyTree",
            params: {selectedItemId: selectedItemId},
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    retrieveVertexLabels: (schemaName, callBack, callBackError) => {
        let url = SqlgGlobal.url + SqlgGlobal.CONTEXT + "schemas/:schemaName/vertexLabels";
        m.request({
            method: "GET",
            url: url,
            params: {
                schemaName: schemaName
            }
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    retrieveEdgeLabels: (schemaName, callBack, callBackError) => {
        let url = SqlgGlobal.url + SqlgGlobal.CONTEXT + "schemas/:schemaName/edgeLabels";
        m.request({
            method: "GET",
            url: url,
            params: {
                schemaName: schemaName
            }
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    retrieveAbstractLabelDetails: (schemaName, abstractLabel, vertexOrEdge, callBack, callBackError) => {
        let url;
        let params = {
            schemaName: schemaName,
            abstractLabel: abstractLabel,
            vertexOrEdge: vertexOrEdge
        };
        url = SqlgGlobal.url + SqlgGlobal.CONTEXT + "schema/:schemaName/:abstractLabel/:vertexOrEdge";
        m.request({
            method: "GET",
            url: url,
            params: params
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    retrieveSchemas: (callBack, callBackError) => {
        let url = SqlgGlobal.url + SqlgGlobal.CONTEXT + "schemas";
        m.request({
            method: "GET",
            url: url
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    retrieveSchemaDetails: (schemaName, callBack, callBackError) => {
        let url;
        let params = {
            schemaName: schemaName
        };
        url = SqlgGlobal.url + SqlgGlobal.CONTEXT + "schema/:schemaName";
        m.request({
            method: "GET",
            url: url,
            params: params
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    renameVertexLabel: (schemaName, oldVertexLabel, newVertexLabel, callBack, callBackError) => {
        let url;
        let params = {
            schemaName: schemaName,
            vertexLabel: oldVertexLabel,
            newVertexLabel: newVertexLabel
        };
        url = SqlgGlobal.url + SqlgGlobal.CONTEXT + "rename/schema/:schemaName/:vertexLabel/:newVertexLabel";
        m.request({
            method: "PUT",
            url: url,
            params: params
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    renameProperties: (schemaName, abstractLabelName, vertexOrEdge, properties, callBack, callBackError) => {
        m.request({
            method: "PUT",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "rename/schema/:schemaName/:abstractLabel/:vertexOrEdge/properties",
            params: {
                schemaName: schemaName,
                abstractLabel: abstractLabelName,
                vertexOrEdge: vertexOrEdge,
            },
            body: properties
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    renameEdgeLabel: (callBack, callBackError) => {

    }
}

export default TopologyManager;