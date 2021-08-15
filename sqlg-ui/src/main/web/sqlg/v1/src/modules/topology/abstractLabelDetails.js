import m from "mithril";
import SlickGrid2 from "../../components/slickgrid/m.slick.grid";
import Button from "../../components/form/button";
import {ELEMENT_TYPE} from "../sqlgModel";

function AbstractLabelDetail(ignore) {

    return {
        oncreate: ({attrs: {state, actions}}) => {
            let properties = document.getElementById('propertiesCollapse')
            properties.addEventListener('show.bs.collapse', function () {
                actions.setPropertiesGridExpand();
                m.redraw();
            });
            properties.addEventListener('hide.bs.collapse', function () {
                actions.setPropertiesGridCollapse();
                m.redraw();
            });
            let indexes = document.getElementById('indexesCollapse')
            indexes.addEventListener('show.bs.collapse', function () {
                actions.setIndexesGridExpand();
                m.redraw();
            });
            indexes.addEventListener('hide.bs.collapse', function () {
                actions.setIndexesGridCollapse();
                m.redraw();
            });
            if (state.topologyDetails.abstractLabel.label === ELEMENT_TYPE.VERTEX_LABEL) {
                let inEdgeLabels = document.getElementById('inEdgeLabelCollapse')
                inEdgeLabels.addEventListener('show.bs.collapse', function () {
                    actions.setInEdgeLabelsGridExpand();
                    m.redraw();
                });
                inEdgeLabels.addEventListener('hide.bs.collapse', function () {
                    actions.setInEdgeLabelsGridCollapse();
                    m.redraw();
                });
                let outEdgeLabels = document.getElementById('outEdgeLabelCollapse')
                outEdgeLabels.addEventListener('show.bs.collapse', function () {
                    actions.setOutEdgeLabelsGridExpand();
                    m.redraw();
                });
                outEdgeLabels.addEventListener('hide.bs.collapse', function () {
                    actions.setOutEdgeLabelsGridCollapse();
                    m.redraw();
                });
            }
            let partitions = document.getElementById('partitionsCollapse')
            partitions.addEventListener('show.bs.collapse', function () {
                actions.setPartitionsGridExpand();
                m.redraw();
            });
            partitions.addEventListener('hide.bs.collapse', function () {
                actions.setPartitionsGridCollapse();
                m.redraw();
            });
        },
        onupdate: ({attrs: {state, actions}}) => {
            actions.setPropertiesGridRefresh();
        },
        view: ({attrs: {state, actions}}) => {
            return m("div.schema-details",
                m("div.row.g-0.mt-1.mb-1.ms-1.me-1", [
                    m("label.col-form-label.col-form-label-sm.col-sm-2", {
                        for: "abstractLabelName"
                    }, "name"),
                    m("div.col-sm-10", [
                        m("input.form-control.form-control-sm", {
                            id: "abstractLabelName",
                            readonly: "",
                            type: "text",
                            value: state.topologyDetails.abstractLabel.name
                        })

                    ])
                ]),
                m("div.schema-header",
                    m("div.col-sm.ms-1.mt-3",
                        m("h4", "Identifiers")
                    )
                ),
                m("div.row.g-0.mt-1.mb-1.ms-1.me-1", [
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
                m("div.d-flex.flex-column.mb-1", [
                    state.topologyDetails.abstractLabel.identifierData.identifiers.map((identifier, index) => {
                        return m("div.row.g-0.mt-1.mb-1.ms-1.me-1", [
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
                ]),
                m("div.schema-header",
                    m("div.ms-1.mt-1.mb-1",
                        m(Button, {
                            class: "bg-info",
                            icon: "fas " + (state.topologyDetails.abstractLabel.propertyColumns.collapsed ? "fa-angle-right" : "fa-angle-down"),
                            text: "Properties",
                            attrs: {
                                "data-bs-toggle": "collapse",
                                "data-bs-target": "#propertiesCollapse"
                            }
                        })
                    )
                ),
                m("div#propertiesCollapse.collapse.schema-details-grid.ms-1.me-1",
                    m(SlickGrid2, {
                        id: 'propertiesGrid',
                        refreshData: state.topologyDetails.abstractLabel.propertyColumns.refresh,
                        rebuildGrid: state.topologyDetails.abstractLabel.propertyColumns.rebuild,
                        showSpinner: state.topologyDetails.abstractLabel.propertyColumns.spin,
                        data: state.topologyDetails.abstractLabel.propertyColumns.data,
                    })
                ),
                m("div.schema-header",
                    m("div.ms-1.mt-1.mb-1",
                        m(Button, {
                            class: "bg-info",
                            icon: "fas " + (state.topologyDetails.abstractLabel.indexes.collapsed ? "fa-angle-right" : "fa-angle-down"),
                            text: "Indexes",
                            attrs: {
                                "data-bs-toggle": "collapse",
                                "data-bs-target": "#indexesCollapse"
                            }
                        })
                    )
                ),
                m("div#indexesCollapse.collapse.schema-details-grid.ms-1.me-1",
                    m(SlickGrid2, {
                        id: 'indexesGrid',
                        refreshData: state.topologyDetails.abstractLabel.indexes.refresh,
                        rebuildGrid: state.topologyDetails.abstractLabel.indexes.rebuild,
                        showSpinner: state.topologyDetails.abstractLabel.indexes.spin,
                        data: state.topologyDetails.abstractLabel.indexes.data,
                    })
                ),
                (state.topologyDetails.abstractLabel.label === ELEMENT_TYPE.VERTEX_LABEL ? [
                        m("div.schema-header",
                            m("div.ms-1.mt-1.mb-1",
                                m(Button, {
                                    class: "bg-info",
                                    icon: "fas " + (state.topologyDetails.abstractLabel.inEdgeLabels.collapsed ? "fa-angle-right" : "fa-angle-down"),
                                    text: "In edge labels",
                                    attrs: {
                                        "data-bs-toggle": "collapse",
                                        "data-bs-target": "#inEdgeLabelCollapse"
                                    }
                                })
                            )
                        ),
                        m("div#inEdgeLabelCollapse.collapse.schema-details-grid.ms-1.me-1",
                            m(SlickGrid2, {
                                id: 'inEdgeLabelsGrid',
                                refreshData: state.topologyDetails.abstractLabel.inEdgeLabels.refresh,
                                rebuildGrid: state.topologyDetails.abstractLabel.inEdgeLabels.rebuild,
                                showSpinner: state.topologyDetails.abstractLabel.inEdgeLabels.spin,
                                data: state.topologyDetails.abstractLabel.inEdgeLabels.data,
                            })
                        ),
                        m("div.schema-header",
                            m("div.ms-1.mt-1.mb-1",
                                m(Button, {
                                    class: "bg-info",
                                    icon: "fas " + (state.topologyDetails.abstractLabel.outEdgeLabels.collapsed ? "fa-angle-right" : "fa-angle-down"),
                                    text: "Out edge labels",
                                    attrs: {
                                        "data-bs-toggle": "collapse",
                                        "data-bs-target": "#outEdgeLabelCollapse"
                                    }
                                })
                            )
                        ),
                        m("div#outEdgeLabelCollapse.collapse.schema-details-grid.ms-1.me-1",
                            m(SlickGrid2, {
                                id: 'outEdgeLabelsGrid',
                                refreshData: state.topologyDetails.abstractLabel.outEdgeLabels.refresh,
                                rebuildGrid: state.topologyDetails.abstractLabel.outEdgeLabels.rebuild,
                                showSpinner: state.topologyDetails.abstractLabel.outEdgeLabels.spin,
                                data: state.topologyDetails.abstractLabel.outEdgeLabels.data,
                            })
                        ),
                    ] :
                    m("div")),
                m("div.schema-header",
                    m("div.ms-1.mt-1.mb-1",
                        m(Button, {
                            class: "bg-info",
                            icon: "fas " + (state.topologyDetails.abstractLabel.partitions.collapsed ? "fa-angle-right" : "fa-angle-down"),
                            text: "Partitions",
                            attrs: {
                                "data-bs-toggle": "collapse",
                                "data-bs-target": "#partitionsCollapse"
                            }
                        })
                    )
                ),
                m("div#partitionsCollapse.collapse.schema-details-grid.ms-1.me-1",
                    m(SlickGrid2, {
                        id: 'partitionsGrid',
                        refreshData: state.topologyDetails.abstractLabel.outEdgeLabels.refresh,
                        rebuildGrid: state.topologyDetails.abstractLabel.outEdgeLabels.rebuild,
                        showSpinner: state.topologyDetails.abstractLabel.outEdgeLabels.spin,
                        data: state.topologyDetails.abstractLabel.outEdgeLabels.data,
                    })
                )
            );
        }
    }
}

export default AbstractLabelDetail;
