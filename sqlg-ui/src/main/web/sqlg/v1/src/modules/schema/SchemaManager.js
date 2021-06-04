import m from 'mithril';
import SqlgGlobal from "../SqlgGlobal";

const SchemaManager = {

    retrieveSchema: (callBack, callBackError) => {
        m.request({
            method: "GET",
            url: SqlgGlobal.url + SqlgGlobal.CONTEXT + "schema"
        }).then(function (data) {
            callBack(data);
        }).catch(function (e) {
            callBackError(e);
        })
    },
}

export default SchemaManager;