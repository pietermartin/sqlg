import m from "mithril";
import Button from "../../components/form/button";
import SlickGrid2 from "../../components/slickgrid/m.slick.grid";
import ButtonPanel from "../../components/buttonPanel/buttonPanel";

function EdgeLabels(ignore) {

    return {
        oninit: ({attrs: {actions}}) => {
            $.Topic('/sqlg-ui/refreshEdgeLabels').subscribe((message) => {
                actions.retrieveEdgeLabels(message['schemaName']);
                if (message.message !== undefined) {
                    actions.message({message: message.message});
                }
                m.redraw();
            });
        },
        view: ({attrs: {state, actions}}) => {
            return m("div.abstract-labels",
                m(SlickGrid2, {
                    id: 'edgeLabelsGrid',
                    refreshData: state.topologyDetails.edgeLabels.refresh,
                    rebuildGrid: state.topologyDetails.edgeLabels.rebuild,
                    showSpinner: state.topologyDetails.edgeLabels.spin,
                    data: state.topologyDetails.edgeLabels.data,
                    deletedItems: state.topologyDetails.edgeLabels.deletedItems,
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
                        enabled: state.editable && state.topologyDetails.edgeLabels.deletedItems.length > 0,
                        onclick: actions.deleteEdgeLabels
                    })
                ])
            );
        }
    }

}

export default EdgeLabels;