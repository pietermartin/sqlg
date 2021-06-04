import m from "mithril";
import SlickLazyTree from "../../components/tree/m.slick.lazytree";
import SchemaManager from "./SchemaManager";

function SchemaTree(ignore) {

    let tree;
    let contextMenuItem;
    let options;
    let defaultOptions = {
        id: "networkVendorTechnologyTreeId",
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
                ElementBrowserManager.retrieveChildren(item, function (data) {
                    callBack(data);
                }, function (e) {
                    CmMithrilGlobal.toasts.push({
                        type: "failure",
                        message: e.message,
                        header: "Element browser",
                        autohide: false
                    });
                    m.redraw();
                });
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
        }
    };

    return {
        oninit: () => {
            options = $.extend(true, {}, defaultOptions);
            SchemaManager.retrieveSchema(function (tree) {
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
        view: () => {
            return m(SlickLazyTree, options)
        }
    }

}

export default SchemaTree;