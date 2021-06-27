import m from "mithril";
//SlickGrid Dependancies
import _ from "underscore";
import $ from "jquery";
import "../../assets/slickgrid/lib/jquery.event.drag-2.3.0";
import "./../slickgrid/jquery-ui/jquery-ui-1.12.1";
import "../../assets/slickgrid/lib/jquery.mousewheel";
//SlickGrid Core files
import "../../assets/slickgrid/slick.core";
import "../../assets/slickgrid/slick.grid";
import "./slick.tree.dataview";
import "../../assets/slickgrid/slick.editors";
import "../../assets/slickgrid/slick.formatters";
// import "./slick.validators";
import "../../assets/slickgrid/slick.groupitemmetadataprovider";
//SlickGrid Controls
import "../../assets/slickgrid/controls/slick.columnpicker";
//SlickGrid Plugins
import "../../assets/slickgrid/plugins/slick.autotooltips";
import "../../assets/slickgrid/plugins/slick.cellcopymanager";
import "../../assets/slickgrid/plugins/slick.cellexternalcopymanager";
import "../../assets/slickgrid/plugins/slick.cellrangedecorator";
import "../../assets/slickgrid/plugins/slick.cellrangeselector";
import "../../assets/slickgrid/plugins/slick.cellselectionmodel";
import "../../assets/slickgrid/plugins/slick.checkboxselectcolumn";
import "../../assets/slickgrid/plugins/slick.headerbuttons";
import "../../assets/slickgrid/plugins/slick.headermenu";
import "../../assets/slickgrid/plugins/slick.rowdetailview";
import "../../assets/slickgrid/plugins/slick.rowmovemanager";
import "../../assets/slickgrid/plugins/slick.rowselectionmodel";
import tippy from 'tippy.js';
import 'tippy.js/dist/tippy.css';
import Input from "../form/input";
import Button from "../form/button";
import "../slickgrid/slickgrid-custom.css"
import "../../assets/slickgrid/slick.grid.css"
import "../../assets/slickgrid/slick-default-theme.css"


function SlickLazyTree(ignore) {

    let selectedItem = null;
    let runFilters;

    let DATAGRID_MIN_WIDTH = 0;

    let isDomInitialized = false;
    let columns = [];
    let data = [];

    let gridPaneWidth;

    let grid = undefined;
    let dataView = undefined;

    /**
     * id,key,showSpinner,
     * data.dataNormal
     * selectedItemCallBack
     * refreshData
     * selectedItem
     *
     * @type {object}
     */
    let defaultOptions = {
        enableCellNavigation: true,
        showHeaderRow: false,
        headerRowHeight: 35,
        explicitInitialization: true,
        editable: false,
        enableAddRow: false,
        asyncEditorLoading: false,
        frozenColumn: 1
    }
    let options = undefined;

    let filterFunction = undefined;
    let columnFilter = null;
    let previousColumnFilter = '';

    let selectedItems;
    let selectedItemCallBack;
    let deselectedItemCallBack;
    let selectedItemsCallBack;
    let deselectedItemsCallBack;
    let tooltip;
    let titleCallBack;
    let tippies = {};
    let gridDiv = '';

    let showSelected = false;

    let contextMenuCallback;
    let fetchChildren;
    let hasChildren;
    let refresh;
    let refreshActive = false;

    let escapeHtml = function (text) {
        let map = {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#039;'
        };
        return text.replace(/[&<>"']/g, function (m) {
            return map[m];
        });
    };

    let ElementFormatter = function (row, cell, value, columnDef, dataContext) {
        value = escapeHtml(value);
        if (!!titleCallBack) {
            value = titleCallBack.title(value, dataContext);
        }
        let spacer = "<span style='display:inline-block;height:1px;width:" + (15 * dataContext["indent"]) + "px'></span>";
        let idx = dataView.getIdxById(dataContext.id);
        let retval;
        let currentItem = dataView.getItems()[idx];
        if (dataContext._collapsed) {
            retval = `${spacer}<i class='${currentItem.isLeaf ? "fas fa-leaf" : "fas fa-caret-right toggle expand"}' aria-hidden='true'></i>`
        } else {
            if (currentItem.spinner === true) {
                retval = `${spacer}<i class='${currentItem.isLeaf ? "fas fa-leaf" : "fas fa-spinner fa-spin toggle collapse"}' aria-hidden='true'></i>`;
            } else {
                retval = `${spacer}<i class='${currentItem.isLeaf ? "fas fa-leaf" : "fas fa-caret-down toggle collapse"}' aria-hidden='true'></i>`;
            }
        }
        if (currentItem.checkBox) {
            retval += "<input class='slick-tree-checkbox' type='checkbox' ";
            if (currentItem.selected) {
                retval += "checked "
            }
            retval += "/>";
        }
        if (tooltip && tooltip.has(currentItem)) {
            let html = tooltip.html(currentItem);
            let tooltipTmp = "<span>" +
                "<span id='tooltip_" + gridDiv.id + "_" + idx + "'>" +
                "<i class='slick-tree-info fas fa-info-circle'></i></span>" +
                "</span>";
            retval += tooltipTmp;
            setTimeout(
                function () {
                    if (tippies[gridDiv.id + "_" + idx] !== undefined) {
                        tippies[gridDiv.id + "_" + idx].destroy();
                        delete tippies[gridDiv.id + "_" + idx];
                    }
                    let tippyCfg = {
                        content: html,
                        allowHTML: true,
                        placement: "right",
                        maxWidth: "30rem",
                        onShown: function (e) {
                            if (tooltip.onShown !== undefined) {
                                tooltip.onShown(e, "#tooltip_" + gridDiv.id + "_" + idx);
                            }
                        }
                    };
                    if (tooltip.event === 'click') {
                        tippyCfg.trigger = "click";
                    }
                    let tippyArray = tippy("#tooltip_" + gridDiv.id + "_" + idx, tippyCfg);
                    if (tippyArray.length > 1) {
                        throw Error("Created more than one tippy for " + gridDiv.id + "_" + idx);
                    }
                    let t = tippyArray[0];
                    tippies[gridDiv.id + "_" + idx] = t;
                }, 0
            );
        }

        if (currentItem['selectAllChildrenCheckBox']) {
            let tooltipTmp = `<span><span id='tooltip_${idx}_selectAll' data-toggle='tooltip' data-placement='right' title='Select/Deselect All'><i class='slick-tree-selectall fas fa-check-double'></i></span></span>`;
            retval += tooltipTmp;
        }

        if (currentItem['icon']) {
            let icon = currentItem['icon'];
            icon = (['fas ', 'far ', 'fad ', 'fal '].some(e => icon.includes(e)) ? icon : "far fa-" + icon);
            retval += `<i class='${icon}'></i>`;
        }
        if (currentItem.isRippable !== undefined && currentItem.isRippable) {
            retval += `<i class='fal fa-cloud-download-alt'></i>`;
        }

        retval += "<span class='slick-cell-show-all'>" + value;
        if (currentItem.badge) {
            retval += `&nbsp;<span class="badge badge-secondary">${currentItem.badge}</span>`
        }
        retval += "</span>";
        return retval;
    };

    let mergeAndSelect = function (elementStack) {
        dataView.mergeAndSelect(elementStack, fetchChildren, completeCallback);
    };

    let completeCallback = function (itemToSelect) {
        let selectedRows = [grid.getData().getRowByItem(itemToSelect)];
        grid.setSelectedRows(selectedRows);
        grid.scrollRowToTop(selectedRows[0]);
    };

    //Basic Filter
    let filter = function (item, args) {
        if (filterFunction) {
            return filterFunction(grid, columnFilter, item, args);
        } else {
            return true;
        }
    };

    /**
     * Private function, calculate the datagrid new height/width from the available space, also consider that a % factor might be applied to calculation
     * object gridOptions
     */
    let calculateGridNewDimensions = function (gridId) {
        // let oneParent = $("#" + gridId).offsetParent();
        // console.log("oneParent = " + oneParent.get(0).id);
        // let twoParent = oneParent.offsetParent();
        // console.log("twoParent = " + twoParent.get(0).id);
        // let newWidth = $("#" + gridId).offsetParent().offsetParent().width();
        let newWidth = $("#" + gridId).offsetParent().width();
        // let h = $("#" + gridId).offsetParent().height();
        // console.log(h);
        // we want to keep a minimum datagrid size, apply these minimum if required
        if (newWidth < DATAGRID_MIN_WIDTH) {
            newWidth = DATAGRID_MIN_WIDTH;
        }
        return {
            width: newWidth
        };
    };

    /** resize the datagrid to fit the browser height & width */
    let resizeToFitBrowserWindow = function (grid, gridId) {
        // calculate new available sizes but with minimum height of 220px
        let newSizes = calculateGridNewDimensions(gridId);
        if (newSizes) {
            // apply these new height/width to the datagrid
            let $grid = $('#' + gridId);
            $grid.height('100%');
            // console.log('setting width ' + newSizes.width);
            $grid.width(newSizes.width);

            let cols = grid.getColumns();
            cols[0].width = newSizes.width;
            grid.setColumns(cols);
            // resize the slickgrid canvas on all browser except some IE versions
            // exclude all IE below IE11
            if (new RegExp('MSIE [6-8]').exec(navigator.userAgent) === null && grid) {
                grid.resizeCanvas();
            }
        }
    };

    let attachAutoResizeDataGrid = function (grid, gridId) {
        let gridDomElm = $('#' + gridId);
        if (!gridDomElm || typeof gridDomElm.offset() === "undefined") {
            // if we can't find the grid to resize, return without attaching anything
            return null;
        }
        //-- 1st resize the datagrid size on first load (because the onResize is not triggered on first page load)
        resizeToFitBrowserWindow(grid, gridId);
        //-- 2nd attach a trigger on the Window DOM element, so that it happens also when resizing after first load
        // $(window).on("resize", function () {
        // 	for some yet unknown reason, calling the resize twice removes any stuttering/flickering when changing the height and makes it much smoother
        // 	self.resizeToFitBrowserWindow(grid, gridId, gridContainerId);
        // 	self.resizeToFitBrowserWindow(grid, gridId, gridContainerId);
        // });
        // in a SPA (Single Page Sqlg) environment you SHOULD also call the destroyAutoResize()
        // let w = $(kendoSplitterDiv);
        let splitter = $("#main").data("splitter");
        if (splitter) {
            splitter.option("onDrag", _.debounce(function () {
                let $grid = $('#' + gridId);
                $grid.width(0);
                setTimeout(function () {
                    resizeToFitBrowserWindow(grid, gridId);
                }, 0);
            }), 1000);
        }
    };

    let setUpColumn = function (column) {
        column.formatter = ElementFormatter;
        column.width = gridPaneWidth / columns.length;
    };

    let filterAndUpdate = function () {
        //We do not allow filtering while using the select all functionality.
        let isNarrowing = previousColumnFilter !== '' && columnFilter.includes(previousColumnFilter);
        let isExpanding = previousColumnFilter !== '' && previousColumnFilter.includes(columnFilter);
        previousColumnFilter = columnFilter;
        dataView.setRefreshHints({
            isFilterNarrowing: isNarrowing,
            isFilterExpanding: isExpanding
        });
        dataView.setFilterArgs({
            fromTree: false,
            isEmpty: columnFilter === '' || columnFilter === undefined || columnFilter === null
        });
        dataView.refresh();
        // if (columnFilter !== '') {
        //     $(this).prev().find('.closeIcon').show();
        // } else {
        //     $(this).prev().find('.closeIcon').hide();
        // }
    };

    let toggle = function (item) {
        if (item) {
            if (item._collapsed && !item._fetched) {
                item._fetched = true;
                if (fetchChildren) {
                    //change carrot to spinner
                    item.spinner = true;
                    fetchChildren(item, function (children) {
                        item.spinner = false;
                        dataView.addChildren(item, children);
                        grid.invalidateAllRows();
                        grid.render();
                    });
                }
            }
            item._collapsed = !item._collapsed;
            dataView.setFilterArgs({item: item, fromTree: true, isTreeCollapsing: item._collapsed});
            dataView.updateItem(item.id, item);
        }
    };

    let asyncPostRender = function (cellNode, row, item, colDef) {
        if (!item._fetched) {
            hasChildren(item, function (result) {
                if (result.hasChildren === false) {
                    item.isLeaf = true;
                    item._fetched = true;
                    $(cellNode).children("i.fa-caret-right").removeClass("fa-caret-right");
                }
            });
        }
    };

    let initializeGrid = function (vnode) {
        if (grid) {
            grid.destroy();
        }

        columns.forEach(col => {
            setUpColumn(col);
        });

        dataView = new Slick.Data.DataTreeView({treeView: true});

        grid = new Slick.Grid(gridDiv, dataView, columns, options);
        grid.setSelectionModel(new Slick.CellSelectionModel({selectActiveRow: true}));
        attachAutoResizeDataGrid(grid, gridDiv.id);
        dataView.syncGridSelection(grid, true, true);

        let copyManager = new Slick.CellExternalCopyManager();
        grid.registerPlugin(copyManager);

        dataView.onRowCountChanged.subscribe(function (e, args) {
            grid.updateRowCount();
            grid.render();
        });

        dataView.onRowsChanged.subscribe(function (e, args) {
            grid.invalidateRows(args.rows);
            grid.render();
        });

        grid.onClick.subscribe(function (e, args) {
            let item = dataView.getItem(args.row);

            if ($(e.target).hasClass("toggle")) {
                toggle(item);
                e.stopImmediatePropagation();
            } else if ($(e.target).is(":checkbox")) {
                //call the timeouts separately so that the checkbox seems responsive.
                window.setTimeout(function (item) {
                    item.selected = !item.selected;
                    if (item.selected) {
                        selectedItems[item.id] = item;
                        if (selectedItemCallBack) {
                            selectedItemCallBack(item, grid, true);
                        }
                    } else {
                        delete selectedItems[item.id];
                        if (deselectedItemCallBack) {
                            deselectedItemCallBack(item, grid);
                        }
                    }
                }, 0, item);
                e.stopImmediatePropagation();
            } else if ($(e.target).hasClass("fa-check-double")) {
                dataView.beginUpdate();
                dataView.selectChildren(item, function (item) {
                    if (item.selected) {
                        selectedItems[item.id] = item;
                        if (selectedItemCallBack) {
                            selectedItemCallBack(item, grid, true);
                        }
                    } else {
                        delete selectedItems[item.id];
                        if (deselectedItemCallBack) {
                            deselectedItemCallBack(item, grid);
                        }
                    }
                });
                dataView.endUpdate();
                e.stopImmediatePropagation();
            } else {
                if (!$(e.target).hasClass("slick-tree-info") && !!vnode.attrs.clickEvent) {
                    vnode.attrs.clickEvent(item);
                }
            }

        });

        grid.onDblClick.subscribe(function (e, args) {
            let item = dataView.getItem(args.row);
            toggle(item);
            e.stopImmediatePropagation();
        });

        grid.onContextMenu.subscribe(function (e) {
            if (contextMenuCallback !== undefined) {
                let cell = grid.getCellFromEvent(e);
                let item = dataView.getItem(cell.row);
                contextMenuCallback(item, e.pageY, e.pageX);
                e.preventDefault();
            }
        });

        $(grid.getHeaderRow()[0]).on("keyup", "input[type=text]", function (e) {
            // if ($.trim($(this).val()).length > 0) {
            //     $('#' + gridDiv.id + ' .closeIcon').show();
            // } else {
            //     $('#' + gridDiv.id + '.closeIcon').hide();
            // }
            columnFilter = $.trim($(this).val());
            window.clearTimeout(runFilters);
            runFilters = window.setTimeout(filterAndUpdate, 100);
        });

        grid.onSelectedRowsChanged.subscribe(function (e, args) {
            if (args.rows.length > 0) {
                let row = args.rows[0];
                selectedItem = dataView.getItem(row);
            } else {
                selectedItem = null;
            }
            if (selectedItemCallBack) {
                selectedItemCallBack(selectedItem, grid, false);
            }
        });

        let slickTooltip = new Slick.AutoTooltips();
        grid.registerPlugin(slickTooltip);

        grid.init();

        dataView.beginUpdate();
        dataView.setItems(data);
        dataView.setFilter(filter);
        dataView.endUpdate();

        if (data.length > 0 && vnode.attrs.selectedItem) {
            let selectedRows = [];
            selectedRows.push(dataView.getRowByItem(vnode.attrs.selectedItem));
            grid.setSelectedRows(selectedRows);
        }
        initializeSelectedItem();

        // remove the column's header
        $('#' + gridDiv.id + " .slick-header-columns").css("height", "0px");
        grid.resizeCanvas();
        return grid;

    };

    let initializeSelectedItem = function () {
        for (let i = 0; i < data.length; i++) {
            let d = data[i];
            if (d.checkBox === true && d.selected === true) {
                selectedItems[d.id] = d;
            }
        }
    };

    return {
        onremove: function () {
        },
        oninit: function (vnode) {
            selectedItems = vnode.attrs.selectedItems;
            selectedItemCallBack = vnode.attrs.selectedItemCallBack;
            deselectedItemCallBack = vnode.attrs.deselectedItemCallBack;
            selectedItemsCallBack = vnode.attrs.selectedItemsCallBack;
            deselectedItemsCallBack = vnode.attrs.deselectedItemsCallBack;
            tooltip = vnode.attrs.tooltip;
            titleCallBack = vnode.attrs.titleCallBack;
        },
        oncreate: function (vnode) {
            if (data && columns && columns.length > 0) {
                let grid = initializeGrid(vnode);
                if (vnode.attrs.slickLazyTreeCallBack) {
                    vnode.attrs.slickLazyTreeCallBack(grid);
                }
                setTimeout(function () {
                    m.redraw();
                }, 0);
            }
            isDomInitialized = true;
            gridDiv = document.querySelector('#gridPane_' + vnode.attrs.id);
        },
        onupdate: function (vnode) {
            vnode.attrs.data.columnFilter = undefined;
        },
        view: function (vnode) {
            refreshActive = vnode.attrs.refreshActive === true;
            if (vnode.attrs.refreshData) {
                data = vnode.attrs.data.dataNormal;
                columns = vnode.attrs.data.columns;
                let tmp = vnode.attrs.data.options ? vnode.attrs.data.options : {};
                options = $.extend({}, defaultOptions, tmp);
                //asyncCheckChildren will use slickgrid's enableAsyncPostRender and asyncPostRender to check is a node has children and if not update the icon
                if (options.asyncCheckChildren === true) {
                    options.enableAsyncPostRender = true;
                    columns[0].asyncPostRender = asyncPostRender;

                    if (!vnode.attrs.hasChildren) {
                        throw Error("asyncCheckChildren is true but no 'hasChildren' function is defined!");
                    }
                    hasChildren = vnode.attrs.hasChildren;
                }
                options.autosizeColsMode = Slick.GridAutosizeColsMode.LegacyForceFit;
                fetchChildren = vnode.attrs.fetchChildren;
                contextMenuCallback = vnode.attrs.contextMenuCallback;
                refresh = vnode.attrs.refresh;

                if (grid) {
                    dataView.beginUpdate();
                    dataView.setItems(data);
                    dataView.setFilter(filter);
                    dataView.setFilterArgs({
                        showAll: false,
                        fromTree: false,
                        showSelectedOnly: false
                    });
                    dataView.endUpdate();
                    if (data.length > 0 && vnode.attrs.selectedItem) {
                        let selectedRows = [];
                        //first clear it then set it, else the event listener won't fire
                        grid.setSelectedRows(selectedRows);
                        selectedRows.push(dataView.getRowByItem(vnode.attrs.selectedItem));
                        grid.setSelectedRows(selectedRows);
                    }
                    initializeSelectedItem();
                    grid.invalidateAllRows();
                    grid.render();
                } else if (isDomInitialized) {
                    let grid = initializeGrid(vnode);
                    if (vnode.attrs.slickLazyTreeCallBack) {
                        vnode.attrs.slickLazyTreeCallBack(grid);
                    }
                }
                vnode.attrs.refreshData = false;
                filterFunction = vnode.attrs.filterFunction;
                dataView.setRefreshHints({
                    isFilterNarrowing: false,
                    isFilterExpanding: false
                });
                dataView.setFilterArgs({
                    showAll: false,
                    fromTree: false,
                    showSelectedOnly: false
                });
            }

            if (vnode.attrs.resizeCanvas && grid) {
                //give the browser time to resize itself
                setTimeout(function () {
                    if (grid) {
                        grid.resizeCanvas();
                    }
                }, 0);
            }

            if (vnode.attrs.mergeAndSelect && Array.isArray(vnode.attrs.mergeAndSelect) && vnode.attrs.mergeAndSelect.length > 0) {
                mergeAndSelect(vnode.attrs.mergeAndSelect, fetchChildren);
            }
            if (vnode.attrs.data.columnFilter && vnode.attrs.data.columnFilter !== '') {
                columnFilter = vnode.attrs.data.columnFilter;
                window.clearTimeout(runFilters);
                runFilters = window.setTimeout(filterAndUpdate, 500);
            }
            return m("div.slick-lazy-tree-container", [
                m("div.lazy-tree-search.input-group.input-group-sm", [
                    m(Input, {
                        id: vnode.attrs.id + "_treeSearch",
                        placeHolder: "Filter",
                        value: columnFilter,
                        "data-tippy-content": "Filter the tree locally",
                        "data-tippy-placement": "top",
                        oninput: function (e) {
                            columnFilter = $.trim(e.target.value);
                            window.clearTimeout(runFilters);
                            runFilters = window.setTimeout(filterAndUpdate, 500);
                        }
                    }),
                    m(Button, {
                        id: vnode.attrs.id + "_ClearFilter",
                        class: "btn-light border",
                        icon: "fa-times",
                        "data-tippy-content": "Clear filter",
                        "data-tippy-placement": "top",
                        enabled: columnFilter && columnFilter !== "",
                        onclick: function (e) {
                            columnFilter = "";
                            dataView.setRefreshHints({
                                isFilterNarrowing: false,
                                isFilterExpanding: true
                            });
                            dataView.setFilterArgs({
                                showAll: false,
                                fromTree: false,
                                showSelectedOnly: showSelected
                            });
                            dataView.refresh();
                            runFilters = window.setTimeout(filterAndUpdate, 0);
                            e.stopImmediatePropagation();
                        }
                    }),
                    m(Button, {
                        id: vnode.attrs.id + "_CollapseTree",
                        class: "btn-light border",
                        icon: "fa-compress-alt",
                        "data-tippy-content": "Collapse tree",
                        "data-tippy-placement": "top",
                        onclick: function (e) {
                            dataView.setFilterArgs({fromTree: false});
                            dataView.setRefreshHints({
                                isFilterNarrowing: false,
                                isFilterExpanding: false,
                                collapseAll: true
                            });
                            dataView.beginUpdate();
                            dataView.collapseAll();
                            dataView.endUpdate();
                            e.stopImmediatePropagation();
                        }
                    }),
                    vnode.attrs.deselectAll ?
                        m(Button, {
                            id: vnode.attrs.id + "_DeselectAll",
                            class: "btn-light border border-left-0 rounded-0",
                            icon: "fa-times-circle",
                            "data-tippy-content": "Deselect all",
                            "data-tippy-placement": "top",
                            enabled: selectedItems && !_.isEmpty(selectedItems),
                            onclick: function (e) {
                                dataView.beginUpdate();
                                dataView.deselectAll(function (item) {
                                    delete selectedItems[item.id];
                                    if (deselectedItemCallBack) {
                                        deselectedItemCallBack(item, grid);
                                    }
                                });
                                dataView.endUpdate();
                                e.stopImmediatePropagation();
                            }
                        }) : m("div"),
                    vnode.attrs.showSelected ?
                        m(Button, {
                            id: vnode.attrs.id + "_ShowSelected",
                            class: (showSelected ? "btn-success" : "btn-light") + " border border-left-0 rounded-0",
                            icon: (!showSelected ? "fa-check-square" : "fa-minus-square"),
                            "data-tippy-content": "Show selected",
                            "data-tippy-placement": "top",
                            onclick: function (e) {
                                showSelected = !showSelected;
                                dataView.setRefreshHints({
                                    isFilterNarrowing: false,
                                    isFilterExpanding: false
                                });
                                dataView.setFilterArgs({
                                    showAll: false,
                                    fromTree: false,
                                    showSelectedOnly: showSelected
                                });
                                dataView.refresh();
                                e.stopImmediatePropagation();
                            }
                        }) : m("div"),
                    !!refresh ?
                        m(Button, {
                            id: vnode.attrs.id + "_Refresh",
                            class: (refreshActive ? "btn-success" : "btn-light") + " border border-left-0 rounded-0",
                            icon: refreshActive ? "fa-sync" : "fa-sync",
                            enabled: true,
                            "data-tippy-content": "Refresh",
                            "data-tippy-placement": "top",
                            onclick: function (e) {
                                vnode.attrs.refresh();
                                e.stopImmediatePropagation();
                                e.redraw = false;
                            }
                        }) : m("div")
                ]),
                m("div.slick-lazy-tree", {id: vnode.attrs.id}, [
                    m("div", {
                            id: "Overlay", class: "overlay-" + vnode.attrs.showSpinner
                        },
                        [m("i.fas.fa-spinner.fa-spin"),]
                    ),
                    m("div", {
                        id: "gridPane_" + vnode.attrs.id,
                        class: "gridPane " + vnode.attrs.gridName,
                        style: 'height: calc(100%); width: 100%;'
                    }),
                ])

            ]);
        }
    }
}

export default SlickLazyTree;
