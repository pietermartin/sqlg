import m from "mithril";
import "./_topologyDetails.scss";

function VertexLabelDetail(ignore) {

    return {
        view: ({attrs: {state, actions}}) => {
            return m("div.schema-details",
                m("div.schema-details-sqlg-schema.container",
                    m("div.row.bg-info",
                        m("div.col-sm",
                            m("h3", state.topologyDetails.vertexLabel.label))
                    ),
                    m("div.row", [
                            m("div.col-sm",
                                m("div", "name")
                            ),
                            m("div.col-sm",
                                m("div", state.topologyDetails.vertexLabel.name)
                            )
                        ]
                    ),
                    m("div.row", [
                            m("div.col-sm",
                                m("div", "ID")
                            ),
                            m("div.col-sm",
                                m("div", state.topologyDetails.vertexLabel.ID)
                            )
                        ]
                    ),
                    m("div.row", [
                            m("div.col-sm",
                                m("div", "createdOn")
                            ),
                            m("div.col-sm",
                                m("div", state.topologyDetails.vertexLabel.createdOn)
                            )
                        ]
                    ),
                    m("div.row",
                        m("div.col-sm",
                            m("h4", "Identifiers"))
                    ),
                    m("div.row", [
                            m("div.col-sm",
                                m("div", "User defined identifiers")
                            ),
                            m("div.col-sm",
                                m("div", state.topologyDetails.vertexLabel.identifierData.userDefinedIdentifiers === true ? "true" : "false")
                            )
                        ]
                    ),
                    state.topologyDetails.vertexLabel.identifierData.identifiers.map((identifier, index) => {
                        return m("div.row", [
                                m("div.col-sm",
                                    m("div", index === 0 ? "Identifier(s)" : "")
                                ),
                                m("div.col-sm",
                                    m("div", identifier)
                                )
                            ]
                        );
                    }),
                    m("div.row",
                        m("div.col-sm",
                            m("h4", "Properties")
                        )
                    ),
                    m("div.row",
                        m("div.col-sm",
                            m("div", "name")
                        ),
                        m("div.col-sm",
                            m("div", "type")
                        )
                    ),
                    state.topologyDetails.vertexLabel.propertyColumns.map(propertyColumn => {
                        return m("div.row", [
                                m("div.col-sm",
                                    m("div", propertyColumn.name)
                                ),
                                m("div.col-sm",
                                    m("div", propertyColumn.type)
                                )
                            ]
                        );
                    }),
                    m("div.row",
                        m("div.col-sm",
                            m("h4", "Indexes")
                        )
                    ),
                    m("div.row",
                        m("div.col-sm",
                            m("div", "name")
                        ),
                        m("div.col-sm",
                            m("div", "type")
                        )
                    ),
                    state.topologyDetails.vertexLabel.indexes.map(index => {
                        let rows = [];
                        rows.push(
                            m("div.row", [
                                m("div.col-sm",
                                    m("div", index.name)
                                ),
                                m("div.col-sm",
                                    m("div", index.type)
                                ),
                            ]));

                        rows.push(
                            m("div.row",
                                m("div.col-sm",
                                    m("h5", "Index properties")
                                )
                            ));
                        for (let i = 0; i < index.properties.length; i++) {
                            let p = index.properties[i];
                            rows.push(
                                m("div.row",
                                    m("div.col-sm",
                                        m("div", "name")
                                    ),
                                    m("div.col-sm",
                                        m("div", p.name)
                                    )
                                ));
                        }
                        return rows;
                    })
                )
            );
        }
    }
}

export default VertexLabelDetail;
