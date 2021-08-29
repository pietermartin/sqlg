import m from "mithril";
import Button from "../../components/form/button";

function SchemaDetail(ignore) {

    return {
        view: ({attrs: {state, actions}}) => {
            return m("div.schema-details", [
                m("div.row.g-0.mt-1.mb-1.ms-1.me-1", [
                    m("label.col-form-label.col-form-label-sm.col-sm-2", {
                        for: "abstractLabelSchemaName"
                    }, "schema"),
                    m("div.col-sm-10", [
                        m("input.form-control.form-control-sm", {
                            id: "abstractLabelSchemaName",
                            readonly: "",
                            type: "text",
                            value: state.topologyDetails.schema.name
                        })
                    ])
                ]),
                m("div.row.g-0.mt-1.mb-1.ms-1.me-1", [
                    m("label.col-form-label.col-form-label-sm.col-sm-2", {
                        for: "abstractLabelName"
                    }, "createdOn"),
                    m("div.col-sm-10", [
                        m("input.form-control.form-control-sm", {
                            id: "abstractLabelName",
                            readonly: "",
                            type: "text",
                            value: state.topologyDetails.schema.createdOn
                        })
                    ]),
                ]),
                state.editable ?
                    m("div.ms-1.mt-3.mb-3",
                        m(Button, {
                            class: "bg-danger",
                            icon: "fas fa-minus-circle",
                            text: "Delete",
                            onclick: actions.deleteSchema
                        })
                    ) :
                    m("div"),
            ]);
        }
    }

}

export default SchemaDetail;