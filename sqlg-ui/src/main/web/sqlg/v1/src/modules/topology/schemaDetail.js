import m from "mithril";
import "./_topologyDetails.scss";

function SchemaDetail(ignore) {

    return {
        view: ({attrs: {state, actions}}) => {
            return m("div.schema-details",
                m("div.schema-details-sqlg-schema.container",
                    m("div.row",
                        m("div.col-sm",
                            m("h2", state.topologyDetails.schema.label))
                    ),
                    m("div.row", [
                            m("div.col-sm",
                                m("div", "name")
                            ),
                            m("div.col-sm",
                                m("div", state.topologyDetails.schema.name)
                            )
                        ]
                    ),
                    m("div.row", [
                            m("div.col-sm",
                                m("div", "ID")
                            ),
                            m("div.col-sm",
                                m("div", state.topologyDetails.schema.ID)
                            )
                        ]
                    ),
                    m("div.row", [
                            m("div.col-sm",
                                m("div", "createdOn")
                            ),
                            m("div.col-sm",
                                m("div", state.topologyDetails.schema.createdOn)
                            )
                        ]
                    )
                )
            );
        }
    }

}

export default SchemaDetail;