import m from "mithril";
import Button from "../../components/form/button";
import SlickGrid2 from "../../components/slickgrid/m.slick.grid";
import ButtonPanel from "../../components/buttonPanel/buttonPanel";

function VertexLabels(ignore) {

    return {
        oninit: ({attrs: {actions}}) => {
            $.Topic('/sqlg-ui/refreshVertexLabels').subscribe((message) => {
                actions.retrieveVertexLabels(message['schemaName']);
                m.redraw();
            });
        },
        view: ({attrs: {state, actions}}) => {
            return m("div.abstract-labels",
                m(SlickGrid2, {
                    id: 'vertexLabelsGrid',
                    refreshData: state.topologyDetails.vertexLabels.refresh,
                    rebuildGrid: state.topologyDetails.vertexLabels.rebuild,
                    showSpinner: state.topologyDetails.vertexLabels.spin,
                    data: state.topologyDetails.vertexLabels.data,
                    deletedItems: state.topologyDetails.vertexLabels.deletedItems,
                    options: {
                        deletionCheckBox: state.editable
                    },
                    deletedItemsCallBack: function (data) {
                        m.redraw();
                    }
                }),
                m(ButtonPanel, {justify: "left"}, [
                    m(Button, {
                        class: "bg-danger",
                        icon: "fas fa-minus-circle",
                        text: "Delete",
                        enabled: state.editable && state.topologyDetails.vertexLabels.deletedItems.length > 0,
                        onclick: actions.deleteVertexLabels
                    })
                ])
            );
        }
    }

}

export default VertexLabels;