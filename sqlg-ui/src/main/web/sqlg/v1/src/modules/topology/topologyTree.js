import m from "mithril";
import SlickLazyTree from "../../components/tree/m.slick.lazytree";
import TopologyManager from "./TopologyManager";

function TopologyTree(ignore) {

    let componentState;
    let componentActions;
    let tree;
    let contextMenuItem;
    let options;
    let defaultOptions = {
        id: "sqlgSchemaTreeId",
        showSpinner: true,
        refreshData: false,
        selectedItems: [],
        data: {
            columns: [{id: "title", name: "Title", field: "title", width: 220}],
            dataNormal: {},
            options: {
                enableCellNavigation: true,
                explicitInitialization: true,
                editable: false,
                enableAddRow: false,
                // asyncCheckChildren: true,
                frozenColumn: 1
            }
        },
        filterFunction: function (grid, columnFilter, item) {
            let result = columnFilter === null || columnFilter === '';
            if (!result) {
                result = item.title.toLowerCase().includes(columnFilter.toLowerCase());
            }
            return result;
        },
        fetchChildren: function (item, callBack) {
            if (item.indent > 1) {
            } else {
                CmMithrilGlobal.toasts.push({
                    type: "warning",
                    message: item.title + " is not in the tree, try refreshing the tree.",
                    header: "Element browser",
                    autohide: false
                });
                m.redraw();
            }
        },
        tooltip: {
            event: 'click',
            has: function (item) {
                return false;
            }, html: function (item) {
                return "<div>halo</div>";
            }
        },
        slickLazyTreeCallBack: function (t) {
            tree = t;
        },
        contextMenuCallback: function (item, y, x) {
            contextMenuItem = item;
            m.redraw();
            let menu = $('#context-menu');
            menu.css({"display": "inline-block"})
            menu.offset({top: y, left: x});
        },
        selectedItemCallBack: function(item) {
            componentActions.retrieveSchemaDetails(item);
            m.redraw();
        }
    };

    return {
        oninit: ({attrs: {state, actions}}) => {
            componentState = state;
            componentActions = actions;
            options = $.extend(true, {}, defaultOptions);
            TopologyManager.retrieveSchema(function (tree) {
                options.showSpinner = false;
                options.refreshData = true;
                options.data.dataNormal = tree;
            }, function (e) {
                console.error(e);
                // CmMithrilGlobal.toasts.push({
                //     type: "failure",
                //     message: e.message,
                //     header: SCREEN_NAME,
                //     autoHide: false
                // });
            });
        },
        view: ({attrs: {state, actions}}) => {
            return m(SlickLazyTree, options)
        }
    }

}

export default TopologyTree;