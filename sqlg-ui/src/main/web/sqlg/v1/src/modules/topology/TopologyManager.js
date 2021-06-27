import m from 'mithril';
import SqlgGlobal from "../SqlgGlobal";

const TopologyManager = {

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
    retrieveSchema: (selectedItemId, callBack, callBackError) => {
        m.request({
            method: "GET",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "schema",
            params: {selectedItemId: selectedItemId},
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
    retrieveSchemaDetails: (item, callBack, callBackError) => {
        let url;
        let params = {};
        switch (item.indent) {
            case 1:
                url = SqlgGlobal.url + SqlgGlobal.CONTEXT + "schema/:schemaName";
                params.schemaName = item.schemaName;
                break;
            case 3:
                url = SqlgGlobal.url + SqlgGlobal.CONTEXT + "schema/:schemaName/:abstractLabel/:vertexOrEdge";
                params.schemaName = item.schemaName;
                params.abstractLabel = item.abstractLabel;
                params.vertexOrEdge = item.vertexOrEdge;
                break;
        }
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
}

export default TopologyManager;