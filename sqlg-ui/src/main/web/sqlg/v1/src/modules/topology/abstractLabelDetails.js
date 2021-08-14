import m from "mithril";
import SlickGrid2 from "../../components/slickgrid/m.slick.grid";

function AbstractLabelDetail(ignore) {

    return {
        onupdate: ({attrs: {state, actions}}) => {
            actions.setPropertiesGridRefresh(false);
        },
        view: ({attrs: {state, actions}}) => {
            return m("div.schema-details",
                m("div.container-fluid",
                    m("div.row.bg-info",
                        m("div.col-sm-12.mt-3.mb-3",
                            m("h4", state.topologyDetails.abstractLabel.label)
                        ),
                    ),
                    m("div.row.mt-3.mb-3.sqlg-schema-details", [
                            m("label.col-form-label.col-form-label.col-sm-2", {
                                for: "abstractLabelName"
                            }, "name"),
                            m("div.col-sm-10",
                                m("input.form-control.form-control-sm", {
                                    id: "abstractLabelName",
                                    readonly: "",
                                    type: "text",
                                    value: state.topologyDetails.abstractLabel.name
                                })
                            )
                        ]
                    ),
                    m("div.row.mt-3.mb-3.sqlg-schema-header",
                        m("div.col-sm.mt-3",
                            m("div.sqlg-schema-header-column",
                                m("h4", "Identifiers")
                            )
                        )
                    ),
                    m("div.row.mb-3.sqlg-schema-details", [
                            m("label.col-form-label.col-form-label-sm.col-sm-2", {
                                for: "userDefined"
                            }, "user defined"),
                            m("div.col-sm-10",
                                m("input.form-control.form-control-sm", {
                                    id: "userDefined",
                                    readonly: "",
                                    type: "text",
                                    value: state.topologyDetails.abstractLabel.identifierData.userDefinedIdentifiers === true ? "true" : "false"
                                })
                            )
                        ]
                    ),
                    state.topologyDetails.abstractLabel.identifierData.identifiers.map((identifier, index) => {
                        return m("div.row.mb-3.sqlg-schema-details", [
                                m("label.col-form-label.col-form-label-sm.col-sm-2", {
                                    for: "identifier" + index
                                }, index === 0 ? "Identifier(s)" : "  "),
                                m("div.col-sm-10",
                                    m("input.form-control.form-control-sm", {
                                        id: "identifier" + index,
                                        readonly: "",
                                        type: "text",
                                        value: identifier
                                    })
                                )
                            ]
                        );
                    }),
                    m("div.row.sqlg-schema-header",
                        m("div.col-sm.mt-3.sqlg-schema-header-column",
                            m("h4", "Properties")
                        )
                    ),
                    m("div.row.sqlg-schema-details.property-columns",
                        m(SlickGrid2, {
                            id: 'propertiesGrid',
                            refreshData: state.topologyDetails.abstractLabel.propertyColumns.refresh,
                            rebuildGrid: false,
                            showSpinner: state.topologyDetails.abstractLabel.propertyColumns.spin,
                            data: state.topologyDetails.abstractLabel.propertyColumns.data,
                        })
                    ),
                    m("div.row.sqlg-schema-header",
                        m("div.col-sm.mt-3.sqlg-schema-header-column",
                            m("h4", "Indexes")
                        )
                    ),
                    m("div.row.sqlg-schema-details.indexes",
                        m(SlickGrid2, {
                            id: 'indexesGrid',
                            refreshData: state.topologyDetails.abstractLabel.indexes.refresh,
                            rebuildGrid: false,
                            showSpinner: state.topologyDetails.abstractLabel.indexes.spin,
                            data: state.topologyDetails.abstractLabel.indexes.data,
                        })
                    )
                )
            );
        }
    }
}

export default AbstractLabelDetail;
