import m from 'mithril';
import $ from "jquery";
import tippy from "tippy.js";
import "../../assets/slickgrid/plugins/slick.autotooltips";

/**
 * @returns {{view: view, oninit: oninit, onremove: onremove, oncreate: oncreate}}
 * @constructor
 */
function SlickSelect() {

    // an extended formatter returns an object { text, removeClasses, addClasses, toolTip }
    // the classes are removed and then added during an update, or just added on cell creation
    let formatter = function pillFormatter(row, cell, value) {
        let rtn;
        if (typeof value === "string" || value instanceof String) {
            rtn = {text: valueToHtml(value)};
        } else {
            rtn = {text: "<h6>" + valueToHtml(value.name) + "&nbsp;<span class=\"badge badge-primary\">" + valueToHtml(value['badge']) + "</span></h6>"};
        }
        return rtn;
    };

    let isDomInitialized = false;
    let isGridInitialized = false;
    let isMultiSelect = false;
    let itemsHaveBadges = true;
    let gridId = undefined;
    let dataView = undefined;
    let data = [];
    let columns = [{id: "name", name: "name", field: "name", formatter: formatter}];
    let gridOpen = false;
    let gridHeight = 200;
    let gridTopPosition = 0;
    let gridLeftPosition = 0;
    let grid = undefined;
    let columnFilters = {};
    let enabled = true;

    let selectedItems = [];
    let selectedItemCallBack = undefined;
    let deletedItemCallBack = undefined;
    let onSelectedRowIdsChanged;
    let gridSyncSetRowSelection;
    let gridSyncGetRowSelection;

    let controlFormBorder = undefined;
    let controlFormBorderRadius = undefined;

    let tooltip;

    //This is set to true for multi select.
    //On a normal click the select box must be toggled
    let toggleAfterClick = false;

    let toggleGrid = function () {
        if (!isGridInitialized) {
            isGridInitialized = true;
            grid.init();
        }
        let gridWidth = calculateGridWidthAndTop();
        gridOpen = !gridOpen;
        let formControl = $("div[id='" + gridId + "Input']");

        if (gridOpen) {
            let caret = formControl.find("i.fa-caret-down");
            caret.removeClass("fa-caret-down");
            caret.addClass("fa-caret-up");
        } else {
            let caret = formControl.find("i.fa-caret-up");
            caret.removeClass("fa-caret-up");
            caret.addClass("fa-caret-down");
        }

        let g = $("div[id='" + gridId + "']");
        if (gridOpen) {
            g.css({
                display: "",
                height: gridHeight + "px",
                position: "absolute",
                top: gridTopPosition,
                left: gridLeftPosition,
                width: gridWidth,
                "z-index": 99999
            });
            controlFormBorder = formControl.css("border-bottom");
            controlFormBorderRadius = formControl.css("border-radius");
            formControl.css({
                "border-bottom-color": "transparent",
                "border-bottom-right-radius": "0",
                "border-bottom-left-radius": "0"
            });
            g.find('[type=text]').focus();
        } else {
            g.css({
                display: "none"
            });
            formControl.css({
                "border-bottom": "",
                "border-radius": ""
            });
            //Clear the filter
            let selectInput = $(grid.getHeaderRow()).find(':input');
            let columnId = selectInput.parent().data("columnId");
            if (columnId != null && columnFilters[columnId] !== undefined && columnFilters[columnId] !== "") {
                columnFilters[columnId] = "";
                selectInput.val("");
                dataView.refresh();
            }
        }

        grid.resizeCanvas();
    };

    let filter = function filter(item) {
        for (let columnId in columnFilters) {
            if (columnId !== undefined && columnFilters[columnId] !== "") {
                let c = grid.getColumns()[grid.getColumnIndex(columnId)];
                let searchValue = columnFilters[columnId];
                if (typeof item[c.field] == 'string' || item[c.field] instanceof String) {
                    if (!item[c.field].toLowerCase().includes(searchValue.toLowerCase())) {
                        return false;
                    }
                } else {
                    if (item[c.field].badge !== undefined) {
                        if (!item[c.field].name.toLowerCase().includes(searchValue.toLowerCase()) &&
                            !item[c.field].badge.toLowerCase().includes(searchValue.toLowerCase())
                        ) {
                            return false;
                        }
                    } else {
                        if (!item[c.field].name.toLowerCase().includes(searchValue.toLowerCase())) {
                            return false;
                        }

                    }
                }
            }
        }
        return true;
    };

    let valueToHtml = function (value) {
        return (value + "")
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;");
    };

    let setItemsHaveBadges = function (data) {
        if (data.length > 0) {
            let firstItem = data[0];
            itemsHaveBadges = !(typeof firstItem.name === "string" || firstItem.name instanceof String);
        } else {
            itemsHaveBadges = false;
        }
    }

    let rowMetadata = function (old_metadata_provider) {
        return function (row) {
            let item = this.getItem(row);
            let ret = old_metadata_provider(row);
            if (item && item.enabled === false) {
                ret = ret || {};
                ret.cssClasses = (ret.cssClasses || '') + ' slick-row-disabled';
            } else {
                ret = ret || {};
            }
            return ret;
        };
    };

    let calculateGridWidthAndTop = function () {
        const current = $("div[id='" + gridId + "']");
        const prev = current.prev();
        let width = prev.get(0).offsetWidth;
        gridTopPosition = prev[0].offsetTop + prev[0].offsetHeight;
        return width;
    };

    let onRowSelectionChange = function (e, args) {
        if (args.ids.length > 0) {
            selectedItems.splice(0, selectedItems.length);
            if (isMultiSelect) {
                for (let i = 0; i < args.ids.length; i++) {
                    let row = args.ids[i];
                    let item = dataView.getItemById(row);
                    if (item.enabled !== undefined && item.enabled === false) {
                        throw Error("item is disabled, this event should not fire");
                    }
                    selectedItems.push(item);
                }
            } else {
                let row = args.ids[0];
                let item = dataView.getItemById(row);
                if (item.enabled !== undefined && item.enabled === false) {
                    throw Error("item is disabled, this event should not fire");
                }
                selectedItems.push(item);
            }
        } else {
            selectedItems.splice(0, selectedItems.length);
        }
        //Occasionally we do not want to redraw on items being selected.
        //In particular if the callBack needs to call m.route.set
        let redraw = true;
        if (isDomInitialized && selectedItemCallBack) {
            redraw = selectedItemCallBack(selectedItems);
            if (redraw === undefined) {
                redraw = true;
            }
        }
        grid.invalidateRows(args.ids);
        grid.render();
        if (toggleAfterClick) {
            toggleAfterClick = false;
            toggleGrid();
        }
        setTimeout(function () {
            if (redraw) {
                m.redraw();
            }
            if (gridOpen) {
                let gridIdDiv = $("div[id='" + gridId + "']")
                const prev = gridIdDiv.prev();
                gridTopPosition = prev[0].offsetTop + prev[0].offsetHeight;
                gridIdDiv.css({top: gridTopPosition});
            }
        }, 0);
    };

    let initializeGrid = function (vnode) {

        if (grid) {
            grid.destroy();
        }
        const options = {
            enableCellNavigation: true,
            enableColumnReorder: false,
            autosizeColsMode: Slick.GridAutosizeColsMode.LegacyForceFit,
            multiSelect: isMultiSelect,
            showHeaderRow: true,
            headerRowHeight: 40,
            explicitInitialization: true  //This indicates that the component will manually call init
        };
        data = vnode.attrs.data;
        if (vnode.attrs.columns) {
            columns = vnode.attrs.columns;
        }

        const current = $("div[id='" + gridId + "']");

        dataView = new Slick.Data.DataView({
            // inlineFilters: true
        });
        dataView.onRowCountChanged.subscribe(function () {
            grid.updateRowCount();
            grid.render();
        });
        dataView.onRowsChanged.subscribe(function (e, args) {
            grid.invalidateRows(args.rows);
            grid.render();
        });
        grid = new Slick.Grid(current, dataView, columns, options);
        grid.onClick.subscribe(function (e, args) {
            let item = dataView.getItem(args.row);
            if (item.enabled !== undefined && item.enabled === false) {
                e.preventDefault();
                e.stopPropagation();
                e.stopImmediatePropagation();
            } else {
                if (isMultiSelect) {
                    if (!e.ctrlKey) {
                        e.ctrlKey = true;
                        toggleAfterClick = true;
                    } else {
                        toggleAfterClick = false;
                    }
                }
            }
        });
        grid.setSelectionModel(new Slick.RowSelectionModel({selectActiveRow: true}));
        let gridSync = dataView.syncGridSelection(grid, true, true);
        onSelectedRowIdsChanged = gridSync.onSelectedRowIdsChanged;
        gridSyncSetRowSelection = gridSync.setSelectedRowIds;
        gridSyncGetRowSelection = gridSync.getSelectedRowIds;
        onSelectedRowIdsChanged.subscribe(
            onRowSelectionChange
        );
        dataView.getItemMetadata = rowMetadata(dataView.getItemMetadata);

        let slickTooltip = new Slick.AutoTooltips();
        grid.registerPlugin(slickTooltip);

        current.css("display", "none");

        grid.onHeaderRowCellRendered.subscribe(function (e, args) {
            $(args.node).empty();
            $(`<div class='input-group no-validate bg-input-group'>
                <input type='text' class='form-control no-validate border-right-0'>
                    <div class='input-group-append'>
                        <div class='input-group-text'><i class='fas fa-search'></i></div>
                    </div>
                </div>`)
                .data("columnId", args.column.id)
                .val(columnFilters[args.column.id])
                .appendTo(args.node);

        });
        $(grid.getHeaderRow()).on("change keyup", ":input", function () {
            let columnId = $(this).parent().data("columnId");
            if (columnId != null) {
                columnFilters[columnId] = $.trim($(this).val());
                dataView.refresh();
            }
        });

        let x = $("#" + gridId);
        x = x.find(".slick-header-columns");
        x.css("height", "0px");

        // grid.init(); init is called when the grid is toggled for the first time.
        dataView.beginUpdate();
        dataView.setItems(data);
        setItemsHaveBadges(data);

        dataView.setFilter(filter);
        dataView.endUpdate();

        if (data.length > 0 && vnode.attrs.selectedItems) {
            selectedItems = vnode.attrs.selectedItems;
            let selectedRows = [];
            for (let i = 0; i < selectedItems.length; i++) {
                let selectedItem = selectedItems[i];
                selectedRows.push(dataView.getRowByItem(selectedItem));
            }
            grid.setSelectedRows(selectedRows);
        }

        $(document).on('click.' + gridId, function (event) {
            if ($(event.target).closest("#" + gridId + "SlickSelectOuter .form-control").length === 0) {
                if (gridOpen) {
                    toggleGrid();
                }
            }
        });

        $(document).on('keyup.' + gridId, function (e) {
            if (e.keyCode === 27 && gridOpen) {
                toggleGrid();
            }
            if (e.keyCode === 13 && $(e.target).closest("#" + gridId).length) {
                if (gridOpen) {
                    toggleGrid();
                }
            }
        });

    };
    return {
        oninit: function (vnode) {
            if (vnode.attrs.id === undefined || vnode.attrs.id === null) {
                throw Error("SlickSelect must have an 'id'");
            }
            if (vnode.attrs.id.indexOf(" ") !== -1 || vnode.attrs.id.indexOf(".") !== -1) {
                throw Error("SlickSelect 'id' may not contain spaces or periods!");
            }
            gridId = vnode.attrs.id;
            isMultiSelect = vnode.attrs.multiSelect ? vnode.attrs.multiSelect : false;
            if (vnode.attrs.gridHeight) {
                gridHeight = vnode.attrs.gridHeight;
            }
            if (vnode.attrs.selectedItemCallBack) {
                selectedItemCallBack = vnode.attrs.selectedItemCallBack;
            }
            if (vnode.attrs.deletedItemCallBack) {
                deletedItemCallBack = vnode.attrs.deletedItemCallBack;
            }
        },
        oncreate: function (vnode) {
            isDomInitialized = true;
            initializeGrid(vnode);
            if (vnode.attrs["data-tippy-content"] !== undefined) {
                tooltip = tippy(document.querySelector('#' + gridId + 'SlickSelectOuter'));
            }
        },
        onremove: function () {
            if (grid) {
                $(document).off("click." + gridId);
                $(document).off("keyup." + gridId);
            }
            if (tooltip !== undefined) {
                tooltip.destroy();
            }
        },
        view: function (vnode) {
            if (vnode.attrs.refresh && isDomInitialized) {
                if (vnode.attrs.refreshCallBack) {
                    vnode.attrs.refreshCallBack();
                }
                data = vnode.attrs.data;
                dataView.beginUpdate();
                dataView.setItems(data);
                setItemsHaveBadges(data);
                dataView.endUpdate();
                if (data.length > 0 && vnode.attrs.selectedItems) {
                    selectedItems = vnode.attrs.selectedItems;
                    let selectedRows = [];
                    for (let i = 0; i < selectedItems.length; i++) {
                        let selectedItem = selectedItems[i];
                        selectedRows.push(dataView.getRowByItem(selectedItem));
                    }
                    grid.setSelectedRows(selectedRows);
                    let selectedRowIds = dataView.mapRowsToIds(selectedRows);
                    gridSyncSetRowSelection(selectedRowIds);
                } else if (data.length === 0) {
                    //clear the selected items
                    selectedItems.splice(0, selectedItems.length);
                    grid.setSelectedRows([]);
                    gridSyncSetRowSelection([]);
                } else {
                    grid.setSelectedRows([]);
                    gridSyncSetRowSelection([]);
                }
                grid.resetActiveCell();
                grid.invalidateAllRows();
                grid.render();
            } else if (vnode.attrs.refreshCallBack && !isDomInitialized) {
                vnode.attrs.refreshCallBack(true);
            }
            if (vnode.attrs.enabled === undefined) {
                enabled = true;
            } else {
                enabled = vnode.attrs.enabled;
            }
            let isValid;
            if (vnode.attrs.valid === undefined) {
                isValid = "";
            } else if (vnode.attrs.valid) {
                isValid = "is-valid";
            } else {
                isValid = "is-invalid";
            }
            let attributes = {};
            if (vnode.attrs["data-tippy-content"]) {
                attributes["data-tippy-content"] = vnode.attrs["data-tippy-content"];
            }
            if (vnode.attrs["data-tippy-placement"]) {
                attributes["data-tippy-placement"] = vnode.attrs["data-tippy-placement"];
            }
            attributes["id"] = vnode.attrs.id + "SlickSelectOuter";
            attributes["class"] = "slick-select-outer";
            if (vnode.attrs["class"]) {
                attributes["class"] = attributes["class"] + " " + vnode.attrs["class"];
            }
            if (isMultiSelect) {
                return m("div", attributes, [
                    m("div", {
                            id: vnode.attrs.id + "Input",
                            class: "form-control form-control-sm " + isValid + (vnode.attrs.class ? " " + vnode.attrs.class : "") + (!enabled ? " text-muted disabled" : ""),
                            tabindex: enabled ? 0 : -1,
                            required: vnode.attrs.required ? "required" : "",
                            onclick: function (e) {
                                if (enabled) {
                                    toggleGrid();
                                }
                                e.redraw = false;
                                e.preventDefault();
                                e.stopPropagation();
                            }
                        }, [
                            selectedItems.length === 0 ? (
                                    itemsHaveBadges ?
                                        m("div", {class: "select-inner-text text-muted"}, [
                                                m("div.slick-select-place-holder.badge.badge-primary", [
                                                        m("i.fas"),
                                                        m("span", vnode.attrs.placeholder, m("span.badge.badge-secondary", "xml"))
                                                    ]
                                                ),
                                                m('i', {class: "fas " + (gridOpen ? "fa-caret-up" : "fa-caret-down") + " float-right"})
                                            ]
                                        ) :
                                        m("div", {class: "select-inner-text text-muted"}, [
                                                m("div.slick-select-place-holder.badge.badge-primary", [
                                                        m("i.fas"),
                                                        m("span", vnode.attrs.placeholder)
                                                    ]
                                                ),
                                                m('i', {class: "fas " + (gridOpen ? "fa-caret-up" : "fa-caret-down") + " float-right"})
                                            ]
                                        )
                                ) :
                                m("div", {class: "select-inner-text"}, [
                                        selectedItems.map(function (selectedItem) {
                                            return m("div", {class: "badge badge-primary"},
                                                m('i', {
                                                    class: "fas fa-times",
                                                    "item-id": selectedItem["id"],
                                                    onclick: function (e) {
                                                        if (enabled) {
                                                            let itemId = e.target.getAttribute("item-id");
                                                            let deletedItem;
                                                            let existingSelectedRowIds = gridSyncGetRowSelection().slice();
                                                            //remove the deleted one from components selectedItems list.
                                                            for (let i = 0; i < selectedItems.length; i++) {
                                                                let item = selectedItems[i];
                                                                if (item.id === itemId) {
                                                                    deletedItem = item;
                                                                    selectedItems.splice(i, 1);
                                                                    break;
                                                                }
                                                            }
                                                            //Remove the deleted one from the internal selection
                                                            for (let i = 0; i < existingSelectedRowIds.length; i++) {
                                                                let id = existingSelectedRowIds[i];
                                                                if (id === itemId) {
                                                                    existingSelectedRowIds.splice(i, 1);
                                                                    break;
                                                                }
                                                            }
                                                            gridSyncSetRowSelection(existingSelectedRowIds);
                                                            let selectedRows = dataView.mapIdsToRows(existingSelectedRowIds);
                                                            grid.setSelectedRows(selectedRows);
                                                            if (deletedItemCallBack !== undefined) {
                                                                deletedItemCallBack(deletedItem);
                                                            }
                                                        }
                                                        e.redraw = true;
                                                        e.preventDefault();
                                                        e.stopPropagation();
                                                    }
                                                }), [
                                                    ((typeof selectedItem.name === "string" || selectedItem.name instanceof String) ?
                                                        " " + valueToHtml(selectedItem.name) :
                                                        m("span", " " + valueToHtml(selectedItem.name.name), [
                                                            m("span", {class: "badge badge-secondary"}, valueToHtml(selectedItem.name.badge))
                                                        ]))
                                                ]
                                            );
                                        })
                                    ],
                                    m('i', {class: "fas " + (gridOpen ? "fa-caret-up" : "fa-caret-down") + " float-right"})
                                )
                        ]
                    ),
                    m("div", {id: vnode.attrs.id, class: "slick-select", style: "display: none;"})
                ]);

            } else {
                return m("div", attributes, [
                    m("div", {
                            id: vnode.attrs.id + "Input",
                            class: "form-control form-control-sm " + isValid + (vnode.attrs.class ? " " + vnode.attrs.class : "") + (!enabled ? " text-muted disabled" : " enabled"),
                            tabindex: enabled ? 0 : -1,
                            required: vnode.attrs.required ? "required" : "",
                            onclick: function (e) {
                                if (enabled) {
                                    toggleGrid();
                                }
                                e.redraw = false;
                            }
                        }, [
                            selectedItems.length === 0 ?
                                m("div", {class: "select-inner-text text-muted"},
                                    m("i.placeholder", vnode.attrs.placeholder),
                                    m('i', {class: "fas " + (gridOpen ? "fa-caret-up" : "fa-caret-down") + " float-right"})
                                ) :
                                selectedItems.map(function (selectedItem) {
                                    return m("div", {class: "select-inner-text"},
                                        m('i', {
                                            class: !vnode.attrs.required ? "fas fa-times" : "",
                                            "item-id": selectedItem["id"],
                                            onclick: function (e) {
                                                if (enabled) {
                                                    let itemId = e.target.getAttribute("item-id");
                                                    let deletedItem;
                                                    let selectedRows = [];
                                                    for (let i = 0; i < selectedItems.length; i++) {
                                                        let item = selectedItems[i];
                                                        if (item.id !== itemId) {
                                                            selectedRows.push(dataView.getRowByItem(item));
                                                        }
                                                    }
                                                    for (let i = 0; i < selectedItems.length; i++) {
                                                        let item = selectedItems[i];
                                                        if (item.id === itemId) {
                                                            deletedItem = item;
                                                            selectedItems.splice(i, 1);
                                                        }
                                                    }
                                                    if (deletedItemCallBack) {
                                                        deletedItemCallBack(deletedItem);
                                                    }
                                                    let selectedRowIds = dataView.mapRowsToIds(selectedRows);
                                                    gridSyncSetRowSelection(selectedRowIds);
                                                    grid.setSelectedRows(selectedRows);
                                                }
                                                e.redraw = true;
                                                e.preventDefault();
                                                e.stopPropagation();
                                            }
                                        }), [
                                            ((typeof selectedItem.name === "string" || selectedItem.name instanceof String) ?
                                                " " + selectedItem.name :
                                                m("span", " " + selectedItem.name.name, [
                                                    m("span", {class: "ml-1 badge badge-secondary"}, selectedItem.name.badge)
                                                ]))
                                        ],
                                        m('i', {class: "fas " + (gridOpen ? "fa-caret-up" : "fa-caret-down") + " float-right"})
                                    );
                                }),
                        ]
                    ),
                    m("div", {id: vnode.attrs.id, class: "slick-select", style: "display: none;"})
                ]);

            }
        }
    }
}

export default SlickSelect;
