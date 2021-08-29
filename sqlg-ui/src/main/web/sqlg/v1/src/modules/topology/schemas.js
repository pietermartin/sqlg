import m from "mithril";
import Button from "../../components/form/button";
import SlickGrid2 from "../../components/slickgrid/m.slick.grid";
import ButtonPanel from "../../components/buttonPanel/buttonPanel";

function Schemas(ignore) {

    return {
        view: ({attrs: {state, actions}}) => {
            return m("div.schemas",
                m(SlickGrid2, {
                    id: 'schemasGrid',
                    refreshData: state.topologyDetails.schemas.refresh,
                    rebuildGrid: state.topologyDetails.schemas.rebuild,
                    showSpinner: state.topologyDetails.schemas.spin,
                    data: state.topologyDetails.schemas.data,
                    deletedItems: state.topologyDetails.schemas.deletedItems,
                    options: {
                        deletionCheckBox: state.editable
                    },
                    deletedItemsCallBack: function (data) {
                        m.redraw();
                    }
                }),
                state.editable ? m(ButtonPanel, {justify: "left"}, [
                    m(Button, {
                        class: "bg-danger",
                        icon: "fas fa-minus-circle",
                        text: "Delete",
                        enabled: state.topologyDetails.schemas.deletedItems.length > 0,
                        onclick: actions.deleteSchemas
                    })
                ]) : m("div")
            );
        }
    }

}

export default Schemas;