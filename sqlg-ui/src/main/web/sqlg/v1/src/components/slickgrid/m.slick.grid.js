// Michael Van Der Merwe 16:58pm - 10/12/2018
import m from "mithril";
import CmMithrilGlobal from "../../utils/CmMithrilGlobal";
import "./countDownSpinner.css";
//SlickGrid Dependancies
import $ from "jquery";
import "../../assets/slickgrid/lib/jquery.event.drag-2.3.0";
import "./jquery-ui/jquery-ui-1.12.1";
import "../../assets/slickgrid/lib/jquery.mousewheel";
//SlickGrid Core files
import "../../assets/slickgrid/slick.core";
import "../../assets/slickgrid/slick.grid";
import "./slick.dataview";
import "../../assets/slickgrid/slick.editors";
import "../../assets/slickgrid/slick.formatters";
import "./slick.validators";
import "../../assets/slickgrid/slick.groupitemmetadataprovider";
//SlickGrid Controls
import "../../assets/slickgrid/controls/slick.columnpicker.alt";
import "./slick.grouppopup";
//SlickGrid Plugins
import "../../assets/slickgrid/plugins/slick.autotooltips";
import "../../assets/slickgrid/plugins/slick.cellcopymanager";
import "../../assets/slickgrid/plugins/slick.cellexternalcopymanager";
import "../../assets/slickgrid/plugins/slick.cellrangedecorator";
import "../../assets/slickgrid/plugins/slick.cellrangeselector";
import "../../assets/slickgrid/plugins/slick.cellselectionmodel";
import "../../assets/slickgrid/plugins/slick.checkboxselectcolumn";
import "./slick.draggablegrouping";
import "../../assets/slickgrid/plugins/slick.headerbuttons";
import "../../assets/slickgrid/plugins/slick.headermenu";
import "../../assets/slickgrid/plugins/slick.rowdetailview";
import "../../assets/slickgrid/plugins/slick.rowmovemanager";
import "../../assets/slickgrid/plugins/slick.rowselectionmodel";
//SlickGrid Formatters
import Formatters from "../slickgrid/formatters";
//SlickGrid CSS files
import "./slick.css";
import "../../assets/slickgrid/slick.grid.css";
import "../../assets/slickgrid/slick-default-theme.css";
import "../../assets/slickgrid/css/smoothness/jquery-ui.css";
// import "../../assets/slickgrid/css/smoothness/jquery-ui-1.11.3.custom.css";
import "../../assets/slickgrid/controls/slick.columnpicker.alt.css";
import "./slickgrid-custom.css";

import moment from "moment";
import _ from "underscore";
import Icon from "../form/icon";
//Slickgrid Custom components
import SlickDropdownFilter from './m.slick.dropdownfilter';
import {createPopper} from '@popperjs/core';

function SlickGrid2() {

    let isDomInitialized = false;
    let preparingFilteredDownload = false;

    let gridId = undefined;
    let gridHeight = 200;
    let selectedItem = null;
    let dblClickCallBack = undefined;
    let selectedItemCallBack = undefined;
    let refreshedCallBack = undefined;

    let filteredItemsLength = 0;
    let checkedItems = [];
    let checkedItemIndex = {};
    let checkedItemsCallBack = undefined;
    let deletedItems = [];
    let deletedItemsCallBack = undefined;
    let newItems = [];
    let newItemsCallBack = undefined;
    let updatedItems = [];
    let updatedItemCallBack = undefined;

    let filterInputCallBack = undefined;

    let options = {};
    let groupMetaOptions = {selectAll: false};
    let columns = [];
    let filteredGroupColumns = [];
    let groupedByColumns = [];
    let data = [];
    let isTree = false;

    let grid = undefined;
    let dataView = undefined;
    let columnFilters;
    let gridCallBack = undefined;

    let globalDeletionState = false;
    let globalDeletionNode;
    let globalSelectionState = false;
    let globalSelectionNode;

    let columnHeaderCache = {};

    let columnsFilter = "";
    let columnsGroupFilterName = "";
    let draggableGrouping;
    let groupsExpanded = false;

    let dropBoxAppendId = "ColumnGroupDropBox";

    let fireFilterFromDropdownInputEvent;
    let selectedDropdownSlickFilterColName = undefined;
    let selectedCmColumn = undefined;
    let selectedDropdownSlickFilterString = undefined;
    let selectedDropdownSlickFilterActive = false;
    let popperInstances = [];

    let reOrderGroups = function (groupId, direction) {
        let oldIndex = groupedByColumns.findIndex((column, index) => {
            if (column.id === groupId) {
                return true
            }
        });

        if (direction && oldIndex !== 0) {
            groupedByColumns.splice((oldIndex - 1), 0, groupedByColumns.splice((oldIndex), 1)[0]);
        } else if (!direction && ((oldIndex + 1) < groupedByColumns.length)) {
            groupedByColumns.splice((oldIndex + 1), 0, groupedByColumns.splice((oldIndex), 1)[0]);
        }
        draggableGrouping.handleGroupBySelection(groupedByColumns);
    };

    let filterColumns = function () {
        let filteredColumns;
        if (columnsFilter.includes(",")) {
            let arr = columnsFilter.split(',');
            let filteredArr = arr.filter(function (el) {
                return el != null && el !== "";
            });
            filteredColumns = columns.filter(function (c) {
                if (filteredArr.some(function (arrayItem) {
                    return (c.id === 'id' ||
                        c.id === 'cm_parent_nodename' ||
                        c.id === 'internal_cm_name' ||
                        c.id.toLowerCase().indexOf(arrayItem.toLowerCase()) > -1);
                })) {
                    return c;
                }
            });
            grid.setColumns(filteredColumns);
        } else {
            filteredColumns = columns.filter(function (c) {
                if (c.id === 'id' || c.id === 'cm_parent_nodename' || c.id === 'internal_cm_name' || c.id.toLowerCase().includes(columnsFilter.toLowerCase())) {
                    return c;
                }
            });
            grid.setColumns(filteredColumns);
        }
    };

    let groupColumnsFilter = function (isNarrowing) {
        if (!isNarrowing) {
            filteredGroupColumns = columns.map((c) => c);
        }
        filteredGroupColumns = filteredGroupColumns.filter(function (c) {
            if (c.id !== "multiSelectCheckBox" && c.id !== "deletion" && c.id.toLowerCase().includes(columnsGroupFilterName.toLowerCase())) {
                return c;
            }
        });
    };

    let toggleGroupCheckboxSelection = function (col, element) {
        let c = getSelectedGridColumn(col.id);
        c['_doGrouping'] = element.checked;
        col['_doGrouping'] = element.checked;

        if (c['_doGrouping'] || col['_doGrouping']) {
            draggableGrouping.addColumnGroupBy(c);
        } else {
            draggableGrouping.removeGroupBy(c.id);
        }
        groupedByColumns = draggableGrouping.getColumnsGroupBy();
    };

    let updateGroupedByColumns = function (args) {
        let dropBox = $("#" + gridId + dropBoxAppendId);
        if (groupedByColumns.findIndex(c => c.id === args.column.id) === -1) {
            args.column['_doGrouping'] = true;
            draggableGrouping.createGroupedEntry(dropBox, args.column);
            draggableGrouping.addColumnGroupBy(args.column);
            groupedByColumns = draggableGrouping.getColumnsGroupBy();
            filteredGroupColumns.forEach(function (col) {
                if (col.id === args.column.id) {
                    col['_doGrouping'] = true;
                }
            });
        } else {
            args.column['_doGrouping'] = false;
            draggableGrouping.removeGroupBy(args.column.id);
            groupedByColumns = draggableGrouping.getColumnsGroupBy();
            filteredGroupColumns.forEach(function (col) {
                if (col.id === args.column.id) {
                    col['_doGrouping'] = false;
                }
            });
        }
    };

    let setGlobalSelectionNodeState = function () {
        if (checkedItems.length > 0) {
            let atLeastOneFilteredItemIsNotChecked = false;
            let atLeastOneFilteredItemIsChecked = false;
            for (let i = 0; i < dataView.getFilteredItems().length; i++) {
                let filteredItem = dataView.getFilteredItems()[i];
                if (checkedItemIndex[filteredItem.id] === undefined) {
                    atLeastOneFilteredItemIsNotChecked = true;
                } else {
                    atLeastOneFilteredItemIsChecked = true;
                }
                if (atLeastOneFilteredItemIsNotChecked && atLeastOneFilteredItemIsChecked) {
                    break;
                }
            }
            if (atLeastOneFilteredItemIsNotChecked && atLeastOneFilteredItemIsChecked) {
                globalSelectionState = true;
                globalSelectionNodePartial();
            } else if (!atLeastOneFilteredItemIsNotChecked && atLeastOneFilteredItemIsChecked) {
                globalSelectionNodeFull();
            } else {
                globalSelectionNodeEmpty();
            }
        } else {
            globalSelectionState = false;
            globalSelectionNodeEmpty();
        }
    };

    let globalSelectionNodePartial = function () {
        globalSelectionNode.removeClass('far');
        globalSelectionNode.removeClass('fa-check-square');
        globalSelectionNode.addClass('fad');
        globalSelectionNode.addClass('fa-square');
    };

    let globalSelectionNodeFull = function () {
        globalSelectionNode.removeClass('fad');
        globalSelectionNode.removeClass('fa-square');
        globalSelectionNode.addClass('far');
        globalSelectionNode.addClass('fa-check-square');
    };

    let globalSelectionNodeEmpty = function () {
        globalSelectionNode.removeClass('fad');
        globalSelectionNode.removeClass('fa-check-square');
        globalSelectionNode.addClass('far');
        globalSelectionNode.addClass('fa-square');
    };

    let isNumber = function (columnType) {
        return columnType === 'SHORT' ||
            columnType === 'INTEGER' ||
            columnType === 'LONG' ||
            columnType === 'FLOAT' ||
            columnType === 'DOUBLE';
    };

    let isDateTime = function (columnType) {
        return columnType === 'LOCALDATETIME';
    };
    let isDate = function (columnType) {
        return columnType === 'LOCALDATE';
    };
    let isTime = function (columnType) {
        return columnType === 'LOCALTIME';
    };

    let getSelectedGridColumn = function (columnId) {
        let c = grid.getColumns()[grid.getColumnIndex(columnId)];
        if (typeof c === "undefined" && columns.length > 0) {
            columns.forEach(function (tmpCol) {
                if (tmpCol.id === columnId) {
                    c = tmpCol;
                }
            });
        }
        return c;
    };

    let getSelectedGridColumnByIndex = function (index, column) {
        let c = grid.getColumns()[index];
        if (typeof c === "undefined" && columns.length > 0) {
            columns.forEach(function (tmpCol) {
                if (tmpCol.id === column.id) {
                    c = tmpCol;
                }
            });
        }
        return c;
    };

    //Basic Filter
    let filter = function filter(item) {
        for (let columnId in columnFilters) {
            if (columnFilters.hasOwnProperty(columnId)) {
                if (columnId === undefined) {
                    throw Error("what!!");
                }
                let c = getSelectedGridColumn(columnId);
                let cmColumn = c['cm_column'];
                if (columnFilters[columnId] !== "") {
                    let itemValue = item[c.field];
                    if (cmColumn !== undefined && cmColumn != null) {
                        if (!cmColumn || cmColumn.type === 'STRING') {
                            if (cmColumn.comparatorValid) {
                                let result = cmColumn.comparator(itemValue);
                                if (!result) {
                                    return false;
                                }
                            } else {
                                if (columnFilters[columnId] === undefined) {
                                    throw Error(`columnFilters property '${columnId}' is undefined!`);
                                }
                                if (!itemValue || !itemValue.toLowerCase().includes(columnFilters[columnId].toLowerCase())) {
                                    return false;
                                }
                            }
                        } else if (cmColumn.type === 'BOOLEAN') {
                            if (columnFilters[columnId] === 'null') {
                                if (itemValue !== null) {
                                    return false;
                                }
                            } else if (columnFilters[columnId] === 'true') {
                                if (!itemValue) {
                                    return false;
                                }
                            } else if (columnFilters[columnId] === 'false') {
                                if (itemValue) {
                                    return false;
                                }
                            }
                        } else if (cmColumn.type === 'SINGLE_SELECT') {
                            if (columnFilters[columnId] === 'null') {
                                if (itemValue !== null) {
                                    return false;
                                }
                            } else {
                                if (!itemValue || !itemValue.toLowerCase().includes(columnFilters[columnId].toLowerCase())) {
                                    return false;
                                }
                            }
                        } else if (isNumber(cmColumn.type)) {
                            if (cmColumn.comparatorValid && !isNaN(itemValue)) {
                                let result = cmColumn.comparator(parseFloat(itemValue));
                                if (!result) {
                                    return false;
                                }
                            }
                            // Use Comparator when building dropdown filters
                        } else if (isDateTime(cmColumn.type)) {
                            if (!itemValue || !itemValue.toLowerCase().includes(columnFilters[columnId].toLowerCase())) {
                                return false;
                            }
                        } else if (isDate(cmColumn.type)) {
                            if (!itemValue || !itemValue.toLowerCase().includes(columnFilters[columnId].toLowerCase())) {
                                return false;
                            }
                        } else if (isTime(cmColumn.type)) {
                            if (!itemValue || !itemValue.toLowerCase().includes(columnFilters[columnId].toLowerCase())) {
                                return false;
                            }
                        } else if (cmColumn.type === 'STRING_ARRAY') {
                            if (itemValue !== null) {
                                if (cmColumn.comparatorValid) {
                                    let result = cmColumn.comparator(itemValue);
                                    if (!result) {
                                        return false;
                                    }
                                } else {
                                    if (columnFilters[columnId] === undefined) {
                                        throw Error(`columnFilters property '${columnId}' is undefined!`);
                                    }
                                    if (!itemValue || !itemValue.toLowerCase().includes(columnFilters[columnId].toLowerCase())) {
                                        return false;
                                    }
                                }
                            } else {
                                return false;
                            }
                        }
                    } else {
                        if (!itemValue || !itemValue.toLowerCase().includes(columnFilters[columnId].toLowerCase())) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    };

    let toggle = function (item) {
        if (item) {
            if (item._collapsed && !item._fetched) {
                item._fetched = true;
            }
            item._collapsed = !item._collapsed;
            dataView.setFilterArgs({item: item, fromTree: true, isTreeCollapsing: item._collapsed});
            dataView.updateItem(item.id, item);
        }
    };

    let validateGrid = function () {
        let rowFailures = {};
        let toValidate = [];
        if (dataView.getNewItems().length > 0) {
            toValidate = toValidate.concat(dataView.getNewItems());
        }
        if (dataView.getUpdatedItems().length > 0) {
            toValidate = toValidate.concat(dataView.getUpdatedItems());
        }
        if (dataView.getDeletedItems().length > 0) {
            toValidate = toValidate.concat(dataView.getDeletedItems());
        }
        for (let r in toValidate) {
            if (toValidate.hasOwnProperty(r)) {
                if (r === 'getItemMetadata') {
                    continue;
                }
                let failures = validateColumns({item: toValidate[r], row: r});
                if (failures.length > 0) {
                    rowFailures[r] = failures;
                }
            }
        }
        if (Object.keys(rowFailures).length > 0) {
            grid.onValidationError.notify({"rowFailures": rowFailures}, new Slick.EventData())
        }
        return Object.keys(rowFailures).length === 0;
    };

    let validateColumns = function (args) {
        let failures = [];
        for (let c in columns) {
            if (columns.hasOwnProperty(c)) {
                let column = columns[c];
                if (column.validator) {
                    let value = args.item[column.id];
                    if (!column.validator(value).valid) {
                        failures.push({columnIndex: c, column: column, rowIndex: args.row, item: args.item})
                    }
                }
            }
        }
        return failures;
    };

    let removeActiveFilter = function (filter) {
        if (columnFilters[filter] !== undefined) {
            columnFilters[filter] = "";
        }
        if (filterInputCallBack) {
            filterInputCallBack(columnFilters);
        }
        grid.setColumns(columns);
        filterColumns();
        dataView.refresh();
        setFilteredItemsLength(dataView, true);
        grid.render();
        toggleFiltersActive();
    };

    let toggleFiltersActive = function () {
        let filteredObject = _.pick(columnFilters, function (value) {
            return value !== undefined && value !== "";
        });
        let filteredColumnNames = $("#" + gridId + "-filtered-column-names");
        filteredColumnNames.removeClass("text-danger");
        filteredColumnNames.empty();
        if (!_.isEmpty(filteredObject)) {
            $("#" + gridId + "-filter-clear-icon").addClass("text-danger");
            $("#" + gridId + "-column-filter-clear-icon").addClass("text-danger");
            let keys = _.keys(filteredObject);
            keys = _.filter(keys, function (v) {
                return v !== undefined && v !== '';
            });
            filteredColumnNames.addClass("text-danger");
            keys.forEach((s) => {
                let filterRemove = $("<i/>", {
                    'class': 'fa fa-times ',
                    'data-filter-remove-id': s,
                    click: function () {
                        removeActiveFilter(s);
                    }
                });
                let filterBadge = $("<span/>", {
                    'class': 'badge bg-secondary text-light',
                    'data-filter-id': s,
                    'html': columns.find(x => x.id === s).name
                }).append(filterRemove);
                filteredColumnNames.append(filterBadge);
            });

        } else {
            $("#" + gridId + "-filter-clear-icon").removeClass("text-danger");
            $("#" + gridId + "-column-filter-clear-icon").removeClass("text-danger");
            filteredColumnNames.append("No filtered columns");
        }
    };

    let setFilteredItemsLength = function (dataView, refresh) {
        filteredItemsLength = dataView.getFilteredItems().length;
        if (filteredItemsLength === data.length) {
            filteredItemsLength = 0;
        }
        if (refresh) {
            if (globalSelectionNode !== undefined && globalSelectionNode !== null) {
                setGlobalSelectionNodeState();
            }
            if (filterInputCallBack) {
                filterInputCallBack(columnFilters);
            } else {
                //Need to redraw to update the filteredItemLength
                setTimeout(function () {
                    m.redraw()
                }, 0);
            }
        }
    };

    let setUpColumnHeader = function (args) {
        $(args.node).empty();
        let columnName = args.column['id'];
        if (args.column.id === "deletion") {
            let input = $("<i class='fas fa-trash-alt'></i>");
            globalDeletionNode = input;
            let wrapped = $("<div style='text-align: center'>");
            input.appendTo(wrapped);
            wrapped.appendTo(args.node);
        } else if (args.column.id === 'multiSelectCheckBox') {
            let input = $("<i class='fas fa-square'></i>");
            globalSelectionNode = input;
            let wrapped = $("<div style='text-align: center'>");
            input.appendTo(wrapped);
            wrapped.appendTo(args.node);
        } else {
            let columnFilter = columnFilters[args.column.id];
            if (args.column['cm_column']) {
                let cmColumn = args.column['cm_column'];
                if (cmColumn.type === "BOOLEAN") {
                    let selectList;
                    selectList = `<SELECT tabIndex='0' class='editor-selectCell${(columnFilter !== undefined && columnFilter !== '') ? ' active-filter' : ''}'>
                                    <OPTION value=""></OPTION>
                                    <OPTION value="null" ${columnFilter === 'null' ? 'selected' : ''}>Null</OPTION>
                                    <OPTION value="true" ${columnFilter === 'true' ? 'selected' : ''}>True</OPTION>
                                    <OPTION value="false" ${columnFilter === 'false' ? 'selected' : ''}>False</OPTION>
                                  </SELECT>`;
                    let jquerySelect = $(selectList);
                    jquerySelect.on('change', function () {
                        let i = $(this);
                        let val = i.val();
                        if (val !== '') {
                            i.addClass("active-filter");
                        } else {
                            i.removeClass("active-filter");
                        }
                        columnFilters[args.column.id] = val;
                        dataView.refresh();
                        setFilteredItemsLength(dataView, true);
                        toggleFiltersActive();
                    });
                    jquerySelect.appendTo(args.node);
                } else if (cmColumn.type === "SINGLE_SELECT") {
                    let selectList;
                    let options = _.map(cmColumn.options, function (o) {
                        return _.extend({
                            text: o.name
                        }, o);
                    });
                    selectList = `<SELECT tabIndex='0' class='editor-selectCell${(columnFilter !== undefined && columnFilter !== '') ? ' active-filter' : ''}'>
                        <OPTION value=""></OPTION>
                        <OPTION value="null">Null</OPTION>`;
                    options.forEach(element => {
                        selectList += `<OPTION value='${element.id}'>${element.name}</OPTION>`;
                    });
                    selectList += `</SELECT>`;
                    let jquerySelect = $(selectList);

                    jquerySelect.val(columnFilter);

                    jquerySelect.on('change', function () {
                        columnFilters[args.column.id] = $(this).val();
                        dataView.refresh();
                        setFilteredItemsLength(dataView, true);
                        toggleFiltersActive();
                    });
                    jquerySelect.appendTo(args.node);
                } else if (isNumber(cmColumn.type)) {
                    let input = $(`<div class='input-group'>
                        <input type="text" class="form-control no-validate" placeholder="Filter" value="${columnFilter !== undefined ? columnFilter : ''}">
                            <span class="input-group-text${(columnFilter !== undefined && columnFilter !== '') ? ' active-filter' : ''} filterAddon">
                                <i class='fas fa-filter'></i>
                            </span>
                    </div>`);
                    if (columnFilter) {
                        let val = columnFilter;
                        cmColumn.comparatorValid = true;
                        if (val.startsWith("([")) {
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + val);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else if (val.includes(",")) {
                            let arr = val.split(',');
                            let filteredArr = arr.filter(function (el) {
                                return el != null && el !== "";
                            });
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + '[' + filteredArr.map(function (i) {
                                    return "\"" + i.toString().toLowerCase() + "\"";
                                }) + ']' + '.some(function(arrayItem){return x === Number(arrayItem); },{x:x});');
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else {
                            try {
                                if (val.indexOf('x') > -1) {
                                } else if (!isNaN(parseInt(val))) {
                                    val = `x===${val}`;
                                } else {
                                    val = 'true';
                                }
                                cmColumn.comparator = new Function('x', 'return ' + val);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                            }
                        }
                    }
                    prepareFilterDropDown(input, columnName, cmColumn);
                    input.find("input").on('input', function () {
                        let i = $(this);
                        let val = i.val();
                        if (val !== '') {
                            i.siblings().children('.input-group-text').addClass("active-filter");
                        } else {
                            i.siblings().children('.input-group-text').removeClass("active-filter");
                        }
                        i.siblings().children('.input-group-text').children('.fa-filter').attr('data-column-filter-string', val);
                        let c = getSelectedGridColumn(args.column.id);
                        let cmColumn = c['cm_column'];
                        columnFilters[args.column.id] = val;
                        cmColumn.comparatorValid = true;

                        if (val.startsWith("([")) {
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + val);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else if (val.includes(",")) {
                            let arr = val.split(',');
                            let filteredArr = arr.filter(function (el) {
                                return el != null && el !== "";
                            });
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + '[' + filteredArr.map(function (i) {
                                    return "\"" + i.toString().toLowerCase() + "\"";
                                }) + ']' + '.some(function(arrayItem){return x === Number(arrayItem); },{x:x});');
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else {
                            try {
                                if (val.indexOf('x') > -1) {
                                } else if (!isNaN(parseInt(val))) {
                                    val = `x===${val}`;
                                } else {
                                    val = 'true';
                                }
                                cmColumn.comparator = new Function('x', 'return ' + val);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        }
                        dataView.refresh();
                        setFilteredItemsLength(dataView, true);
                        toggleFiltersActive();
                    });
                    input.appendTo(args.node);
                } else if (cmColumn.type === "STRING_ARRAY") {
                    let input;
                    if (columnFilter === undefined) {
                        input = columnHeaderCache["cmColumnTextHeader"];
                        if (columnHeaderCache["cmColumnTextHeader"] === undefined) {
                            input = $(`<div class='input-group'>
                                         <input type="text" class="form-control no-validate " placeholder="Filter" value="">
                                             <span class="input-group-text filterAddon">
                                               <i class='fas fa-filter'></i>
                                          </span>
                                        </div>`);
                            columnHeaderCache["cmColumnTextHeader"] = input;
                        } else {
                            input = columnHeaderCache["cmColumnTextHeader"].clone();
                            let inputInput = input.find("input:text");
                            inputInput.val('');
                            inputInput.siblings().children('.input-group-text').removeClass("active-filter");
                        }
                    } else {
                        input = $(`<div class='input-group'>
                                     <input type="text" class="form-control no-validate " placeholder="Filter" value="${columnFilter}">
                                       <span class="input-group-text${(columnFilter !== '') ? ' active-filter' : ''} filterAddon">
                                         <i class='fas fa-filter'></i>
                                       </span>
                                   </div>`);
                    }
                    prepareFilterDropDown(input, columnName, cmColumn);
                    input.find("input").on('input', function () {
                        let i = $(this);
                        let val = i.val();
                        if (val !== '') {
                            i.siblings().children('.input-group-text').addClass("active-filter");
                        } else {
                            i.siblings().children('.input-group-text').removeClass("active-filter");
                        }
                        i.siblings().children('.input-group-text').children('.fa-filter').attr('data-column-filter-string', val);
                        if (val.startsWith("([")) {
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + val);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else if (val.includes(",")) {
                            let arr = val.split(',');
                            let filteredArr = arr.filter(function (el) {
                                return el != null && el !== "";
                            });
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x',
                                    `let excluded = false;` + '[' + filteredArr.map(function (i) {
                                        return "\"" + i.toString().toLowerCase() + "\"";
                                    }) + ']' + `.forEach(function(searchItem){
                                        x.forEach(function(arrayItem){
                                            if(excluded){return;}
                                            if(arrayItem.toLowerCase().includes(searchItem.toLowerCase())){
                                                excluded = true;
                                            }});
                                });
                                return excluded;`
                                );
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else {
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            cmColumn.comparatorValid = true;

                            //We do not allow filtering while using the select all functionality.
                            let previousColumnFilter = columnFilters[args.column.id];
                            let columnFilter = val;

                            try {
                                cmColumn.comparator = new Function('x', `
                                let excluded = false;
                                x.forEach(function(arrayItem){
                                if(excluded){return;}
                                if(arrayItem.toLowerCase().includes("` + columnFilter + `".toLowerCase())){
                                excluded = true;
                                }});
                                return excluded;
                                `);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }

                            let isNarrowing = previousColumnFilter !== undefined && previousColumnFilter !== '' && columnFilter.includes(previousColumnFilter);
                            let isExpanding = previousColumnFilter !== undefined && previousColumnFilter !== '' && previousColumnFilter.includes(columnFilter);
                            columnFilters[args.column.id] = val;
                            dataView.setRefreshHints({
                                isFilterNarrowing: isNarrowing,
                                isFilterExpanding: isExpanding
                            });
                            if (isTree) {
                                dataView.setFilterArgs({
                                    fromTree: false,
                                    isEmpty: val === '' || val === undefined || val === null
                                });
                            }
                        }
                        dataView.refresh();
                        setFilteredItemsLength(dataView, true);
                        toggleFiltersActive();
                    });
                    input.appendTo(args.node);
                } else {
                    let input;
                    if (columnFilter === undefined) {
                        input = columnHeaderCache["cmColumnTextHeader"];
                        if (columnHeaderCache["cmColumnTextHeader"] === undefined) {
                            input = $(`<div class='input-group'>
                                         <input type="text" class="form-control no-validate " placeholder="Filter" value="">
                                             <span class="input-group-text filterAddon">
                                               <i class='fas fa-filter'></i>
                                             </span>
                                        </div>`);
                            columnHeaderCache["cmColumnTextHeader"] = input;
                        } else {
                            input = columnHeaderCache["cmColumnTextHeader"].clone();
                            let inputInput = input.find("input:text");
                            inputInput.val('');
                            inputInput.siblings().children('.input-group-text').removeClass("active-filter");
                        }
                    } else {
                        input = $(`<div class='input-group' data-filter-column="${columnName}">
                                     <input type="text" class="form-control no-validate " placeholder="Filter" value="${columnFilter}">
                                       <span class="input-group-text${(columnFilter !== '') ? ' active-filter' : ''} filterAddon">
                                         <i class='fas fa-filter'></i>
                                       </span>
                                   </div>`);
                    }
                    prepareFilterDropDown(input, columnName, cmColumn);
                    input.find("input").on('input', function () {
                        let i = $(this);
                        let val = i.val();
                        if (val !== '') {
                            i.siblings().children('.input-group-text').addClass("active-filter");
                        } else {
                            i.siblings().children('.input-group-text').removeClass("active-filter");
                        }
                        i.siblings().children('.input-group-text').children('.fa-filter').attr('data-column-filter-string', val);
                        if (val.startsWith("([")) {
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + val);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else if (val.includes("startsWith") || val.includes("endsWith") || val.includes("includes")) {
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + val);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else if (val.includes("x ") || val.includes("x ") || val.includes("x.")) {
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + val);
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else if (val.includes(",")) {
                            let arr = val.split(',');
                            let filteredArr = arr.filter(function (el) {
                                return el != null && el !== "";
                            });
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            columnFilters[args.column.id] = val;
                            cmColumn.comparatorValid = true;
                            try {
                                cmColumn.comparator = new Function('x', 'return ' + '[' + filteredArr.map(function (i) {
                                    return "\"" + i.toString().toLowerCase() + "\"";
                                }) + ']' + '.some(function(arrayItem){return x.toLowerCase().indexOf(arrayItem.toLowerCase()) > -1;},{x:x});');
                            } catch (e) {
                                cmColumn.comparatorValid = false;
                                return true;
                            }
                        } else {
                            let c = getSelectedGridColumn(args.column.id);
                            let cmColumn = c['cm_column'];
                            cmColumn.comparatorValid = false;

                            //We do not allow filtering while using the select all functionality.
                            let previousColumnFilter = columnFilters[args.column.id];
                            let columnFilter = val;

                            let isNarrowing = previousColumnFilter !== undefined && previousColumnFilter !== '' && columnFilter.includes(previousColumnFilter);
                            let isExpanding = previousColumnFilter !== undefined && previousColumnFilter !== '' && previousColumnFilter.includes(columnFilter);
                            columnFilters[args.column.id] = val;
                            dataView.setRefreshHints({
                                isFilterNarrowing: isNarrowing,
                                isFilterExpanding: isExpanding
                            });
                            if (isTree) {
                                dataView.setFilterArgs({
                                    fromTree: false,
                                    isEmpty: val === '' || val === undefined || val === null
                                });
                            }
                        }
                        dataView.refresh();
                        setFilteredItemsLength(dataView, true);
                        toggleFiltersActive();
                    });
                    input.appendTo(args.node);
                }
            } else {
                let input = $(`<input type='text' class='form-control no-validate' value='${columnFilter !== undefined ? columnFilter : ''}'>`);
                input.on('input', function () {
                    columnFilters[args.column.id] = $(this).val();
                    dataView.refresh();
                    setFilteredItemsLength(dataView, true);
                    toggleFiltersActive();
                });
                input.appendTo(args.node);
            }
        }
    };

    let setUpColumn = function (column, index) {
        if (column['cm_column']) {
            let cmColumn = column['cm_column'];
            if (index === 0 && isTree === true) {
                column['formatter'] = Formatters.TreeFormatter;
                column["header"] = {
                    buttons: [
                        {
                            cssClass: "fas fa-compress-arrows-alt",
                            command: "collapse-tree",
                            tooltip: "Collapse the tree.",
                        }, {
                            cssClass: "fas fa-expand-arrows-alt",
                            command: "expand-tree",
                            tooltip: "Expand the tree.",
                        }
                    ]
                };
            } else if (cmColumn.isTree === true || cmColumn.type === "STRING") {
                if (cmColumn['isEditable'] && cmColumn.options) {
                    column['editor'] = Slick.Editors.SelectOption;
                    column['options'] = cmColumn.options;
                } else if (cmColumn['isEditable']) {
                    column['editor'] = Slick.Editors.Text;
                }
                if (cmColumn.isRequired) {
                    column['validator'] = Slick.Validators.RequiredTextValidator;
                    column['formatter'] = Formatters.RequiredFormatter;
                }
            } else if (cmColumn.type === "STRING_ARRAY") {
                if (cmColumn['isEditable']) {
                    if (!cmColumn.options) {
                        throw Error(`column ${column} is of type 'STRING_ARRAY' and is editable but has no 'options'`);
                    }
                    column['editor'] = Slick.Editors.SelectOption;
                    column['options'] = cmColumn.options;
                }
                if (cmColumn.isRequired) {
                    column['validator'] = Slick.Validators.RequiredTextValidator;
                    column['formatter'] = Formatters.RequiredFormatter;
                }
            } else if (cmColumn.type === "BOOLEAN") {
                column['formatter'] = Formatters.CheckboxFormatter;
                if (cmColumn['isEditable']) {
                    column['editor'] = Slick.Editors.Checkbox;
                }
            } else if (cmColumn.type === "SINGLE_SELECT") {
                if (cmColumn['isEditable']) {
                    column['editor'] = Slick.Editors.SelectOption;
                    column['formatter'] = Formatters.SelectCellFormatter;
                }
                column['options'] = cmColumn.options;
            } else if (isNumber(cmColumn.type)) {
                column['formatter'] = Formatters.NumberFormatter;
                if (cmColumn['isEditable']) {
                    column['editor'] = Slick.Editors.Integer;
                }
            } else if (isDateTime(cmColumn.type)) {
                column['formatter'] = Formatters.LocalDateTimeFormatter;
                if (cmColumn['isEditable']) {
                }
            } else if (isDate(cmColumn.type)) {
                column['formatter'] = Formatters.LocalDateFormatter;
                if (cmColumn['isEditable']) {
                }
            } else if (isTime(cmColumn.type)) {
                column['formatter'] = Formatters.LocalTimeFormatter;
                if (cmColumn['isEditable']) {
                }
            }
            if (cmColumn["icon"]) {
                column["header"] = {buttons: [{cssClass: "fas " + cmColumn["icon"]}]};
            }
        }
        if (column['id'] === "deletion") {
            column['formatter'] = Formatters.DeletionFormatter;
        }
        if (column['id'] === "multiSelectCheckBox") {
            column['formatter'] = Formatters.SelectRowFormatter;
        }


        column['sortable'] = isTree !== true;
        column['autoSize'] = {
            ignoreHeaderText: true,
            // colValueArray: undefined,
            // allowAddlPercent: undefined,
            // formatterOverride: undefined,
            autosizeMode: Slick.ColAutosizeMode.LegacyForceFit,
            // rowSelectionModeOnInit: undefined,
            // rowSelectionMode: Slick.RowSelectionMode.FirstNRows,
            // rowSelectionCount: 100,
            // valueFilterMode: Slick.ValueFilterMode.None,
            widthEvalMode: Slick.WidthEvalMode.CanvasTextSize,
            sizeToRemaining: true,
            // widthPx: undefined,
            // colDataTypeOf: undefined
        };
        if (column['id'] === 'id') {
            column['autoSize'] = undefined;
        }
        if (column["header"] === undefined) {
            column["header"] = {
                buttons: [
                    {
                        cssClass: "fas fa-bars",
                        command: "groupby-column"
                    }]
            };
        } else {
            column["header"]["buttons"].push({
                cssClass: "fas fa-bars",
                command: "groupby-column"
            });

        }
    };


    /**
     * add a css class to turn the deletion icon red
     * @param old_metadata_provider
     * @returns {function(*=): *}
     */
    let rowMetadata = function (old_metadata_provider) {
        return function (row) {
            let item = this.getItem(row);
            let ret = old_metadata_provider(row);
            if (item && item.deletion) {
                ret = ret || {};
                ret.cssClasses = (ret.cssClasses || '') + ' slick-row-deleted';
            } else if (item && item.selected) {
                ret = ret || {};
                ret.cssClasses = (ret.cssClasses || '') + ' slick-row-selected';
            } else {
                ret = ret || {};
            }
            if (item && item._isNew) {
                ret = ret || {};
                ret.cssClasses = (ret.cssClasses || '') + ' slick-row-new';
            }
            return ret;
        };
    };

    let prepareFilterDropDown = function (input, columnName, cmColumn) {
        let span = input.find("span").get(0);
        let dropDown = $(`#SlickDropdownFilter_${gridId}`).get(0);
        let popperInstance = createPopper(span, dropDown, {
            placement: "bottom",
            modifiers: [
                {
                    name: 'flip',
                    enabled: false,
                },
                {
                    name: 'offset',
                    options: {
                        offset: [0, 8],
                    },
                },
                {
                    name: 'focus',
                    enabled: true,
                    phase: 'afterWrite',
                    fn({state}) {
                        if (state.elements.popper.getAttribute("data-show") !== null) {
                            $(state.elements.popper).find("input")[1].focus();
                        }
                    }
                }
            ]
        });
        popperInstances.push(popperInstance);
        span.addEventListener("click", function (e) {
            let inputBox = input.find("input");
            let val = inputBox.val();
            selectedDropdownSlickFilterColName = columnName;
            selectedCmColumn = cmColumn;
            selectedDropdownSlickFilterString = val;
            selectedDropdownSlickFilterActive = true;
            fireFilterFromDropdownInputEvent = function (value) {
                inputBox.val(value);
                inputBox.trigger("input");
                selectedDropdownSlickFilterActive = false;
            }
            dropDown.setAttribute('data-show', '');
            popperInstance.update();
            // //update popper only after a redraw so the dom is properly created for it to calculate its position
            // setTimeout(() => {
            //     popperInstance.update();
            // }, 0);
            m.redraw();
            e.preventDefault();
            e.stopPropagation();
        });
    }

    let initializeGrid = function (vnode) {

        if (grid) {
            grid.destroy();
        }

        data = vnode.attrs.data ? vnode.attrs.data.data : [];
        columns = vnode.attrs.data.columns ? vnode.attrs.data.columns : [];

        if (vnode.attrs.gridCallBack) {
            gridCallBack = vnode.attrs.gridCallBack;
        }
        let tmp = vnode.attrs.options ? vnode.attrs.options : {};

        draggableGrouping = new Slick.DraggableGrouping({
            dropbox: "#" + gridId + dropBoxAppendId,
            iconImage: '../images/delete.png',
            groupIconImage: '../images/column-grouping.png',
            dropPlaceHolderText: 'Drag here to set row groups'
        });
        draggableGrouping.onGroupChanged.subscribe(function (e, args) {
            if (args.groupColumns.length > 0) {
                $("#" + gridId + "-group-clear").addClass("text-danger");
            } else {
                $("#" + gridId + "-group-clear").removeClass("text-danger");
            }
        });

        let defaultOptions = {
            //16px is slick grids hardcoded value. Here we allow for changing it
            // cssSlickHeaderColumnHeight: '16px',
            cssSlickHeaderColumnHeight: '22px',

            showHeaderRow: true,
            headerRowHeight: 30,
            //PreHeader
            createPreHeaderPanel: false,
            showPreHeaderPanel: false,
            preHeaderPanelHeight: 35,

            enableCellNavigation: true,
            explicitInitialization: true,
            autoEdit: false,
            editable: false,
            enableAddRow: false,
            asyncEditorLoading: false,

            // autosizeColsMode: Slick.GridAutosizeColsMode.FitViewportToCols,
            // autosizeColsMode: Slick.GridAutosizeColsMode.FitColsToViewport,
            autosizeColsMode: Slick.GridAutosizeColsMode.LegacyForceFit,
            autosizeColPaddingPx: 4,
            autosizeTextAvgToMWidthRatio: 0.75,
            viewportSwitchToScrollModeWidthPercent: undefined,
            viewportMinWidthPx: undefined,
            viewportMaxWidthPx: undefined,

            multiSelect: false,
            selectActiveRow: false,
            deletionCheckBox: false,
            multiSelectCheckBox: false,
            multiColumnSort: true,
            numberedMultiColumnSort: true,
            tristateMultiColumnSort: true,
            enableColumnReorder: true,
            columnPicker: {
                //columnTitle: "Columns",
                fadeSpeed: 0,
                hideForceFitButton: true,
                hideSyncResizeButton: true,
                headerColumnValueExtractor: function (columnDef) {
                    if (columnDef.id === 'multiSelectCheckBox') {
                        return "Select row";
                    } else if (columnDef.id === 'deletion') {
                        return "Delete row";
                    } else {
                        return columnDef.name;
                    }
                }
            },
        };
        options = $.extend({}, defaultOptions, tmp);
        //This is to hav a bigger header height to accommodate wrapping text for the header. comparator has an example.
        document.documentElement.style.setProperty('--slick-header-column-height', options.cssSlickHeaderColumnHeight);

        isTree = options.isTree === true;
        if (isTree === true) {
            options.frozenColumn = 1;
        } else {
            options.frozenColumn = undefined;
        }
        if (columns.length > 0) {
            if (options.deletionCheckBox && _.filter(columns, function (o) {
                return o.id === "deletion";
            }).length === 0) {
                columns.push({
                    id: "deletion",
                    field: "deletion",
                    name: "",
                    minWidth: 30,
                    maxWidth: 30
                });
            }
            if (options.multiSelectCheckBox && columns[0].id !== "multiSelectCheckBox") {
                columns.unshift({
                    id: "multiSelectCheckBox",
                    field: "multiSelectCheckBox",
                    name: "",
                    minWidth: 30,
                    maxWidth: 30
                });
                groupMetaOptions.selectAll = true;
            }
            for (let i = 0; i < columns.length; i++) {
                let c = columns[i];
                setUpColumn(c, i);
            }
        }

        let groupItemMetadataProvider;
        if (isTree) {
            dataView = new Slick.Data.DataTreeView({treeView: true});
        } else {
            groupItemMetadataProvider = new Slick.Data.GroupItemMetadataProvider(groupMetaOptions);
            dataView = new Slick.Data.DataView({groupItemMetadataProvider: groupItemMetadataProvider});
        }

        dataView.onRowCountChanged.subscribe(function (e, args) {
            grid.updateRowCount();
            grid.render();
        });
        dataView.onRowsChanged.subscribe(function (e, args) {
            grid.invalidateRows(args.rows);
            grid.render();
        });

        grid = new Slick.Grid("#" + gridId, dataView, columns, options);
        // grouping = new Slick.Controls.SlickGridGroupPopUp(dataView, grid);
        if (groupItemMetadataProvider !== undefined) {
            grid.registerPlugin(groupItemMetadataProvider);
        }
        // grid.setSelectionModel(new Slick.RowSelectionModel({selectActiveRow: true}));
        grid.setSelectionModel(new Slick.CellSelectionModel({selectActiveRow: true}));

        grid.registerPlugin(draggableGrouping);

        grid.onViewportChanged.subscribe(function (e, args) {
            if (vnode.attrs.viewPortChanged) {
                vnode.attrs.viewPortChanged(e, args);
            }
        });

        new Slick.Controls.ColumnPicker(columns, grid, options);

        dataView.syncGridSelection(grid, true, true);
        dataView.getItemMetadata = rowMetadata(dataView.getItemMetadata);

        let copyManager = new Slick.CellExternalCopyManager();
        grid.registerPlugin(copyManager);

        let slickTooltip = new Slick.AutoTooltips();
        grid.registerPlugin(slickTooltip);

        // let headerButtonsPlugin = new Slick.Plugins.HeaderButtons({buttonCssClass: "cm-slick-custom-button"});
        let headerButtonsPlugin = new Slick.Plugins.HeaderButtons({});
        headerButtonsPlugin.onCommand.subscribe(function (e, args) {
            let command = args.command;
            if (command === "collapse-tree") {
                let items = dataView.getItems();
                for (let i = 2; i < items.length; i++) {
                    items[i]._collapsed = true;
                }
                dataView.setFilterArgs({
                    showAll: false,
                    fromTree: false,
                    showSelectedOnly: false
                });
                dataView.setRefreshHints({
                    isFilterNarrowing: false,
                    isFilterExpanding: false,
                    collapseAll: true
                });
                dataView.refresh();
            } else if (command === "expand-tree") {
                dataView.setFilterArgs({
                    showAll: true,
                    fromTree: false,
                    showSelectedOnly: false
                });
                dataView.refresh();
            } else if (command === "groupby-column") {
                updateGroupedByColumns(args);
            }
        });
        grid.registerPlugin(headerButtonsPlugin);

        grid.onHeaderRowCellRendered.subscribe(function (e, args) {
            setUpColumnHeader(args);
        });

        grid.onSort.subscribe(function (e, args) {
            let cols = args.sortCols;
            data.sort(function (dataRow1, dataRow2) {
                for (let i = 0, l = cols.length; i < l; i++) {
                    let field = cols[i].sortCol.field;
                    let cmColumn = cols[i].sortCol["cm_column"];
                    let sign = cols[i].sortAsc ? 1 : -1;
                    let value1 = dataRow1[field], value2 = dataRow2[field];
                    if (!cmColumn || cmColumn.type === 'STRING' || cmColumn.type === 'BOOLEAN' || cmColumn.type === 'SINGLE_SELECT') {
                        let result = (value1 === value2 ? 0 : (value1 > value2 ? 1 : -1)) * sign;
                        if (result !== 0) {
                            return result;
                        }
                    } else if (cmColumn && (cmColumn.type === 'LOCALDATE' || cmColumn.type === 'LOCALDATETIME')) {
                        let date1 = moment(value1);
                        let date2 = moment(value2);
                        let result = (date1.isSame(date2) ? 0 : (date1.isAfter(date2) ? 1 : -1)) * sign;
                        if (result !== 0) {
                            return result;
                        }
                    } else if (cmColumn.type === 'LOCALTIME') {
                        let date1 = moment(value1, moment.HTML5_FMT.TIME_SECONDS);
                        let date2 = moment(value2, moment.HTML5_FMT.TIME_SECONDS);
                        let result = (date1.isSame(date2) ? 0 : (date1.isAfter(date2) ? 1 : -1)) * sign;
                        if (result !== 0) {
                            return result;
                        }
                    } else if (cmColumn && (isNumber(cmColumn.type))) {
                        let number1 = parseFloat(value1);
                        let number2 = parseFloat(value2);
                        let result = (number1 === number2 ? 0 : (number1 > number2 ? 1 : -1)) * sign;
                        if (result !== 0) {
                            return result;
                        }
                    } else if (cmColumn.type === 'STRING_ARRAY') {
                        let result, valueString1, valueString2;
                        if (value1 === null) {
                            valueString1 = '<NULL>';
                        } else {
                            valueString1 = value1.toString();
                        }
                        if (value2 === null) {
                            valueString2 = '<NULL>';
                        } else {
                            valueString2 = value2.toString();
                        }
                        result = (valueString1 === valueString2 ? 0 : (valueString1 > valueString2 ? 1 : -1)) * sign;
                        if (result !== 0) {
                            return result;
                        }
                    }
                }
                return 0;
            });

            dataView.beginUpdate();
            dataView.setItems(data);
            dataView.endUpdate();
            grid.invalidateAllRows();
            grid.render();
        });

        $(grid.getHeaderRow()).on("click", ".fa-trash-alt", function (e) {
            globalDeletionState = !globalDeletionState;
            if (Slick.GlobalEditorLock.commitCurrentEdit()) {
                let data;
                let gridData = grid.getData().getItems();
                let filteredData = dataView.getFilteredItems();
                if (gridData.length === filteredData.length) {
                    data = gridData;
                } else {
                    data = filteredData;
                }
                let newToDelete = [];
                let rows = [];
                for (let i = 0; i < data.length; i++) {
                    rows.push(i);
                    let item = data[i];
                    //check if the item is a new item, if so just remove it
                    let wasNew = false;
                    for (let i = 0; i < grid.getData().getNewItems().length; i++) {
                        let newItem = grid.getData().getNewItems()[i];
                        if (newItem.id === item.id) {
                            newToDelete.push(item);
                            wasNew = true;
                            break;
                        }
                    }
                    if (!wasNew) {
                        if (item.deletion) {
                            for (let i = 0; i < deletedItems.length; i++) {
                                if (deletedItems[i].id === item.id) {
                                    deletedItems.splice(i, 1);
                                }
                            }
                        } else {
                            deletedItems.push(item);
                        }
                        item.deletion = globalDeletionState;
                        if (deletedItemsCallBack) {
                            deletedItemsCallBack({add: null, remove: item, items: deletedItems});
                        }
                    }
                }
                for (const newToDeleteElement of newToDelete) {
                    grid.getData().deleteItem(newToDeleteElement.id);
                }
                if (globalDeletionState) {
                    globalDeletionNode.addClass('slick-row-deleted');
                } else {
                    globalDeletionNode.removeClass('slick-row-deleted');
                }
                grid.invalidateRows(rows);
                grid.render();
            }
        });
        $(grid.getHeaderRow()).on("click", ".fa-check-square,.fa-square", function (e) {
            globalSelectionState = !globalSelectionState;
            if (Slick.GlobalEditorLock.commitCurrentEdit()) {
                // let data = grid.getData().getItems();
                let data = dataView.getFilteredItems();
                if (data.length > 0) {
                    let data = grid.getData().getItems();
                }
                let removedItems = [];
                // if (!globalSelectionState) {
                //     removedItems = checkedItems.slice();
                // }
                // checkedItems.splice(0);
                let filteredCheckItemCount = 0;
                for (let i = 0; i < data.length; i++) {
                    let item = data[i];
                    item.selected = globalSelectionState;
                    if (globalSelectionState && checkedItemIndex[item.id] === undefined) {
                        checkedItems.push(item);
                        checkedItemIndex[item.id] = item;
                        filteredCheckItemCount++;
                    } else if (!globalSelectionState && checkedItemIndex[item.id] !== undefined) {
                        delete checkedItemIndex[item.id];
                    }
                }
                if (!globalSelectionState) {
                    //iterate backwards for deletions to work nicely
                    for (let i = checkedItems.length - 1; i >= 0; i--) {
                        for (let j = 0; j < data.length; j++) {
                            let item = data[j];
                            if (checkedItems[i].id === item.id) {
                                checkedItems.splice(i, 1);
                                break;
                            }
                        }
                    }
                }
                if (globalSelectionState && filteredCheckItemCount === data.length) {
                    globalSelectionNodeFull();
                } else if (globalSelectionState && filteredCheckItemCount > 0) {
                    globalSelectionNodePartial();
                } else {
                    globalSelectionNodeEmpty();
                }
                let rows = [];
                for (let i = 0; i < grid.getData().getLength(); i++) {
                    rows.push(i);
                }
                grid.invalidateRows(rows);
                grid.render();
                if (checkedItemsCallBack) {
                    checkedItemsCallBack({add: null, remove: removedItems, items: checkedItems});
                }
            }
        });
        grid.onSelectedRowsChanged.subscribe(function (e, args) {
            if (args.rows.length > 0) {
                let row = args.rows[0];
                selectedItem = dataView.getItem(row);
            }
            if (selectedItemCallBack) {
                selectedItemCallBack(selectedItem);
            }
        });
        grid.onCellChange.subscribe(function (e, args) {
            let column = grid.getColumns()[args.cell];
            dataView.updateItem(args.item.id, args.item, column.field);

            let found = false;
            for (let i = 0; i < updatedItems.length; i++) {
                let updatedItem = updatedItems[i];
                if (updatedItem.id === args.item.id) {
                    found = true;
                    break;
                }
            }
            if (!found && !args.item._isNew) {
                updatedItems.push(args.item);
            }
            if (updatedItemCallBack && !args.item._isNew) {
                updatedItemCallBack(args);
            }
        });
        grid.onClick.subscribe(function (e, args) {
            if (Slick.GlobalEditorLock.commitCurrentEdit()) {
                let item = dataView.getItem(args.row);
                if ($(e.target).hasClass("toggle")) {
                    toggle(item);
                    e.stopImmediatePropagation();
                } else if (item && !item.__group) {
                    let column = grid.getColumns()[args.cell];
                    if (column.id === 'deletion') {
                        let row = args.row;
                        let item = grid.getData().getItem(row);

                        //check if the item is a new item, if so just remove it
                        let wasNew = false;

                        if (grid.getData().getNewItemsIdx !== undefined && grid.getData().getNewItemsIdx()[item.id] !== undefined) {
                            grid.getData().deleteItem(item.id);
                            let indexToDelete = newItems.indexOf(item);
                            newItems.splice(indexToDelete, 1);
                            wasNew = true;
                        }

                        if (!wasNew) {
                            if (item['deletion']) {
                                item['deletion'] = false;
                                globalDeletionState = false;
                                globalDeletionNode.removeClass('slick-row-deleted');
                                for (let i = 0; i < deletedItems.length; i++) {
                                    if (deletedItems[i].id === item.id) {
                                        deletedItems.splice(i, 1);
                                    }
                                }
                                if (deletedItemsCallBack) {
                                    deletedItemsCallBack({add: null, remove: item, items: deletedItems});
                                }
                            } else {
                                item['deletion'] = true;
                                item['selected'] = false;
                                deletedItems.push(item);
                                if (deletedItemsCallBack) {
                                    deletedItemsCallBack({add: item, remove: null, items: deletedItems});
                                }
                            }
                        }
                        grid.invalidateRow(row);
                        grid.render();
                        e.stopImmediatePropagation();
                    } else if (column.id === 'multiSelectCheckBox') {
                        let row = args.row;
                        let item = grid.getData().getItem(row);
                        //ignore rows marked for deletion
                        if (!item.deletion) {
                            if (item['selected']) {
                                item['selected'] = false;
                                for (let i = 0; i < checkedItems.length; i++) {
                                    if (checkedItems[i].id === item.id) {
                                        delete checkedItemIndex[item.id];
                                        checkedItems.splice(i, 1);
                                    }
                                }
                                if (checkedItemsCallBack) {
                                    checkedItemsCallBack({add: null, remove: item, items: checkedItems});
                                }
                            } else {
                                item['selected'] = true;
                                checkedItems.push(item);
                                checkedItemIndex[item.id] = item;
                                if (checkedItemsCallBack) {
                                    checkedItemsCallBack({add: item, remove: null, items: checkedItems});
                                }
                            }
                            setGlobalSelectionNodeState();
                            grid.invalidateRow(row);
                            grid.render();
                        }
                        e.stopImmediatePropagation();
                    }
                } else if (item && item.__group) {
                    e.preventDefault();
                    let groupRows = item.rows;
                    if ($(e.target).data("selection")) {
                        groupRows.forEach(function (row) {
                            let item = row;
                            item['selected'] = true;
                            checkedItems.push(item);
                            checkedItemIndex[item.id] = item;
                            if (checkedItemsCallBack) {
                                checkedItemsCallBack({add: item, remove: null, items: checkedItems});
                            }
                        });
                    } else {
                        groupRows.forEach(function (row) {
                            let item = row;
                            if (item['selected']) {
                                item['selected'] = false;
                                for (let i = 0; i < checkedItems.length; i++) {
                                    if (checkedItems[i].id === item.id) {
                                        delete checkedItemIndex[item.id];
                                        checkedItems.splice(i, 1);
                                    }
                                }
                                if (checkedItemsCallBack) {
                                    checkedItemsCallBack({add: null, remove: item, items: checkedItems});
                                }
                            }
                        });
                    }
                    setGlobalSelectionNodeState();
                    grid.invalidateAllRows(groupRows);
                    grid.render();
                    e.stopImmediatePropagation();
                }
            }
        });

        grid.onDblClick.subscribe(function (e, args) {
            if (Slick.GlobalEditorLock.commitCurrentEdit()) {
                let item = dataView.getItem(args.row);
                if (isTree === false && dblClickCallBack !== undefined) {
                    dblClickCallBack(item);
                } else if (isTree === true) {
                    let item = dataView.getItem(args.row);
                    toggle(item);
                    e.stopImmediatePropagation();
                }
            }
        });

        grid.onColumnsResized.subscribe(function (e, args) {
            let c = grid.getColumns()[grid.getColumnIndex(args.triggeredByColumn)];
            columns.forEach(function (col) {
                if (c.name.toLowerCase() === col.name.toLowerCase()) {
                    col.width = c.width;
                }
            });
            grid.setOptions({autosizeColsMode: Slick.GridAutosizeColsMode.None});
            e.stopImmediatePropagation();
            e.stopPropagation();
        });

        grid.onBeforeAppendCell.subscribe(function (e, args) {
            let cmColumn = grid.getColumns()[args.cell]['cm_column'];
            if (cmColumn && cmColumn['cellColorMap']) {
                //returns the css class to append to the cell
                //the map is a map of rows and colors
                return cmColumn['cellColorMap'][args.dataContext.id];
            }
        });

        grid.onAddNewRow.subscribe(function (e, args) {
            let item = args.item;
            grid.invalidateRow(data.length);
            item.id = dataView.getItems().length + 1;
            dataView.addItem(item);
            item._isNew = true;
            newItems.push(item);
            grid.updateRowCount();
            grid.render();
            if (newItemsCallBack) {
                newItemsCallBack(item);
            }
        });

        grid.validate = validateGrid;

        grid.init();

        dataView.beginUpdate();
        dataView.setItems(data);
        dataView.setFilter(filter);
        dataView.endUpdate();
        grid.autosizeColumns();
        toggleFiltersActive();

        if (gridCallBack) {
            gridCallBack(grid);
        }

        if (data.length > 0) {
            if (vnode.attrs.selectedItem) {
                selectedItem = vnode.attrs.selectedItem;
                let selectedRows = [dataView.getRowByItem(selectedItem)];
                // grid.setSelectedRows(selectedRows);
                grid.setSelectedRow(selectedRows[0]);
                grid.scrollRowToTop(selectedRows[0]);
            }
            if (vnode.attrs.checkedItems) {
                checkedItems = vnode.attrs.checkedItems;
            }
            if (vnode.attrs.deletedItems) {
                deletedItems = vnode.attrs.deletedItems;
            }
            if (vnode.attrs.newItems) {
                newItems = vnode.attrs.newItems;
            }
            if (vnode.attrs.updatedItems) {
                updatedItems = vnode.attrs.updatedItems;
            }
        }

    };

    let exportDataFromGrid = function (columns, filtered, context, filename) {
        /// Clean the columns before exporting
        let cleanColumns = columns.filter(function (col) {
            if (col.field !== "deletion" && col.field !== "multiSelectCheckBox") {
                return col;
            }
        });
        let url = `${CmMithrilGlobal.url}${CmMithrilGlobal.CONTEXT}cm/data/slickgrid/download/:context/:filename/export`;
        m.request({
            method: "POST",
            url: url,
            body: {data: filtered ? dataView.getFilteredItems() : dataView.getItems(), columns: cleanColumns},
            params: {
                context: context,
                filename: encodeURIComponent(filename)
            },
        }).then(function (data) {
            preparingFilteredDownload = false;
            let a = document.createElement("a");
            let attribute = document.createAttribute("href");
            attribute.value = encodeURIComponent(`cm/data/slickgrid/download/${context}/${data.fileName}`);
            a.setAttributeNode(attribute);
            a.click();
            a.remove();
        }).catch(function (e) {
            preparingFilteredDownload = false;
            CmMithrilGlobal.toasts.push({
                type: "failure",
                message: e.message,
            });
        });
    };

    let exportFilteredRowsFromXlsxFile = function (context, filename) {
        let ids = _.pluck(dataView.getFilteredItems(), "id");
        let url = `${CmMithrilGlobal.url}${CmMithrilGlobal.CONTEXT}cm/data/slickgrid/download/:context/:filename/exportFilteredXlsx`;
        m.request({
            method: "POST",
            url: url,
            body: ids,
            params: {
                context: context,
                filename: encodeURIComponent(filename)
            },
        }).then(function (data) {
            preparingFilteredDownload = false;
            let a = document.createElement("a");
            let attribute = document.createAttribute("href");
            attribute.value = `cm/data/slickgrid/download/${context}/${filename.substring(0, filename.length - 5)}_filtered.xlsx`;
            a.setAttributeNode(attribute);
            a.click();
            a.remove();
        }).catch(function (e) {
            preparingFilteredDownload = false;
            CmMithrilGlobal.toasts.push({
                type: "failure",
                message: e.message,
            });
        });

    };

    let resizeNotification = function () {
        if (grid) {
            grid.resizeCanvas();
        }
    };

    let aggregate = function (col) {
        if (col['_doAggregate']) {
            if (col['_aggregation'] === undefined) {
                col['_aggregation'] = 'sum';
            }
            col['groupTotalsFormatter'] = reductionToAggregateFormatter(col['_aggregation'])
            col['aggregators'] = [reductionToAggregator(col['_aggregation'], col.id)];
            draggableGrouping.addReductionToGrouping(col);
        } else {
            draggableGrouping.removeReductionToGrouping(col);
            col['aggregators'] = [];
        }
    }

    let reductionToAggregator = function (reduction, field) {
        switch (reduction) {
            case 'sum':
                return new Slick.Data.Aggregators.Sum(field);
            case 'avg':
                return new Slick.Data.Aggregators.Avg(field);
            case 'min':
                return new Slick.Data.Aggregators.Min(field);
            case 'max':
                return new Slick.Data.Aggregators.Max(field);
            default:
                throw Error("Unknown reduction '" + reduction + "'");
        }
    }

    let reductionToAggregateFormatter = function (reduction) {
        switch (reduction) {
            case 'sum':
                return Formatters.SumTotalsFormatter;
            case 'avg':
                return Formatters.AvgTotalsFormatter;
            case 'min':
                return Formatters.MinTotalsFormatter;
            case 'max':
                return Formatters.MaxTotalsFormatter;
            default:
                throw Error("Unknown reduction '" + reduction + "'");
        }
    }

    return {
        oninit: function (vnode) {
            $.Topic('footer-resize').subscribe(resizeNotification);
            gridId = vnode.attrs.id;
            if (vnode.attrs.gridHeight) {
                gridHeight = vnode.attrs.gridHeight;
            }
            if (vnode.attrs.selectedItem) {
                selectedItem = vnode.attrs.selectedItem;
            }
            if (vnode.attrs.selectedItemCallBack) {
                selectedItemCallBack = vnode.attrs.selectedItemCallBack;
            }
            if (vnode.attrs.dblClickCallBack) {
                dblClickCallBack = vnode.attrs.dblClickCallBack;
            }
            if (vnode.attrs.checkedItems) {
                checkedItems = vnode.attrs.checkedItems;
            }
            if (vnode.attrs.checkedItemsCallBack) {
                checkedItemsCallBack = vnode.attrs.checkedItemsCallBack;
            }
            if (vnode.attrs.deletedItems) {
                deletedItems = vnode.attrs.deletedItems;
            }
            if (vnode.attrs.deletedItemsCallBack) {
                deletedItemsCallBack = vnode.attrs.deletedItemsCallBack;
            }
            if (vnode.attrs.newItems) {
                newItems = vnode.attrs.newItems;
            }
            if (vnode.attrs.newItemsCallBack) {
                newItemsCallBack = vnode.attrs.newItemsCallBack;
            }
            if (vnode.attrs.updatedItems) {
                updatedItems = vnode.attrs.updatedItems;
            }
            if (vnode.attrs.updatedItemCallBack) {
                updatedItemCallBack = vnode.attrs.updatedItemCallBack;
            }
            if (vnode.attrs.filterInputCallBack) {
                filterInputCallBack = vnode.attrs.filterInputCallBack;
            }
            if (vnode.attrs.refreshedCallBack) {
                refreshedCallBack = vnode.attrs.refreshedCallBack;
            }
        },
        oncreate: function (vnode) {
            if (data && columns && columns.length > 0) {
                initializeGrid(vnode);
                setFilteredItemsLength(dataView, false);
                setTimeout(function () {
                    m.redraw();
                }, 0);
            }
            isDomInitialized = true;
        },
        onupdate: function (vnode) {
        },
        onremove: function (vnode) {
            $.Topic('footer-resize').unsubscribe(resizeNotification);
            if (grid) {
                grid.destroy();
                grid = null;
            }
            if (columns.length > 0) {
                columns.forEach(function (c) {
                    c['_doGrouping'] = undefined;
                    if (c["header"] !== undefined) {
                        c["header"]["buttons"] = []
                    }
                });
            }
            for (let i = 0; i < popperInstances.length; i++) {
                popperInstances[i].destroy();
            }
        },
        view: function (vnode) {
            if (vnode.attrs.rebuildGrid) {
                if (!vnode.attrs.data) {
                    throw Error("Can not rebuild a grid that has no data.");
                }
                data = vnode.attrs.data.data;
                columns = vnode.attrs.data.columns;
                filteredGroupColumns = columns.map((c) => c);
                columnFilters = vnode.attrs.columnFilters ? vnode.attrs.columnFilters : {};
                updatedItems = vnode.attrs.updatedItems ? vnode.attrs.updatedItems : [];
                if (isDomInitialized) {
                    initializeGrid(vnode);
                    setFilteredItemsLength(dataView, false);
                }
                if (vnode.attrs.refreshedCallBack) {
                    vnode.attrs.refreshedCallBack();
                }
            } else if (vnode.attrs.refreshData) {
                data = vnode.attrs.data.data;
                columns = vnode.attrs.data.columns;
                newItems = vnode.attrs.newItems;
                if (vnode.attrs.deletedItems) {
                    deletedItems = vnode.attrs.deletedItems;
                }
                filteredGroupColumns = columns.map((c) => c);
                columnFilters = vnode.attrs.columnFilters ? vnode.attrs.columnFilters : {};
                updatedItems = vnode.attrs.updatedItems ? vnode.attrs.updatedItems : [];
                if (grid) {
                    dataView.beginUpdate();
                    dataView.setItems(data);
                    if (newItems && grid.getData().getNewItems().length < newItems.length) {
                        for (const newItem of newItems) {
                            if (grid.getData().getNewItemsIdx()[newItem.id] === undefined) {
                                newItem._isNew = true;
                                grid.getData().addItem(newItem);
                            }
                        }
                    }
                    dataView.setFilter(filter);
                    dataView.setFilterArgs({
                        showAll: false,
                        fromTree: false,
                        showSelectedOnly: false
                    });
                    dataView.endUpdate();
                    if (data.length > 0 && vnode.attrs.selectedItem) {
                        let selectedRows = [];
                        selectedRows.push(dataView.getRowByItem(selectedItem));
                        grid.setSelectedRows(selectedRows);
                    }
                    grid.invalidateAllRows();
                    grid.render();
                } else if (isDomInitialized) {
                    initializeGrid(vnode);
                    setFilteredItemsLength(dataView, true);
                }
                if (vnode.attrs.refreshedCallBack) {
                    vnode.attrs.refreshedCallBack();
                }
                if (dataView !== undefined) {
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
            }
            if (vnode.attrs.resizeCanvas && grid) {
                //give the browser time to resize itself
                setTimeout(function () {
                    if (grid) {
                        grid.resizeCanvas();
                    }
                }, 0);
            }


            let tmp = {
                group: true,
                export: true,
                showRows: true,
                edit: false,
                save: false
            };
            let gridTools = $.extend({}, tmp, vnode.attrs.gridTools);

            let footerComponents = [];
            let groupHeaderComponents = [
                m("div.slick-filtered-column", [
                        m("span.slick-grid-header-icon.ps-1",
                            m(Icon, {
                                id: gridId + "-column-filter-clear-icon",
                                class: `fas fa-filter`,
                                "data-tippy-content": "Clear individual column filters",
                                "data-tippy-placement": "top",
                                onclick: function (e) {
                                    for (let i = 0; i < columns.length; i++) {
                                        if (columnFilters[columns[i].id] !== undefined) {
                                            columnFilters[columns[i].id] = "";
                                        }
                                    }
                                    if (filterInputCallBack) {
                                        filterInputCallBack(columnFilters);
                                    }
                                    grid.setColumns(columns);
                                    dataView.refresh();
                                    setFilteredItemsLength(dataView, true);
                                    grid.render();
                                    toggleFiltersActive();
                                    e.redraw = false;
                                }
                            })
                        ),
                        m("div.slick-filtered-column-names.text-muted", {id: gridId + "-filtered-column-names"})
                    ]
                ),
                // m("div.slick-grid-header-dropbox", {id: gridId + "ColumnGroupDropBox"}),
                m("span.slick-grid-header-icon", [
                    m("div.dropdown", [
                        m(Icon, {
                            id: gridId + "-group-columns",
                            class: `fas fa-layer-group dropdown-toggle ${groupedByColumns.length > 0 ? "text-primary" : ''}`,
                            "data-tippy-content": "Choose columns to group",
                            "data-tippy-placement": "top",
                            attrs: {"data-bs-toggle": "dropdown"},
                            "aria-haspopup": "true",
                            "aria-expanded": "false"
                        }),
                        m("div.dropdown-menu.dropdown-menu-sm.text-center", {
                                onclick: function (e) {
                                    e.stopPropagation();
                                }
                            },
                            m("div.slick-grid-header-group-filter-selected-columns", [
                                groupedByColumns.map(function (col) {
                                    return col['_doGrouping'] ? m("div.dropdown-item", [
                                        m("div.custom-dropdown-section-selected", [
                                            m("label", {for: gridId + col.id}, col.name),
                                            m(Icon, {
                                                class: "fas fa-times group-column-deselect",
                                                "data-tippy-content": "Clear Item",
                                                "data-tippy-placement": "top",
                                                onclick: function (e) {
                                                    let c = getSelectedGridColumn(col.id);
                                                    c['_doGrouping'] = false;
                                                    col['_doGrouping'] = false;
                                                    for (let i = 0; i < filteredGroupColumns.length; i++) {
                                                        if (filteredGroupColumns[i].id === col.id) {
                                                            filteredGroupColumns[i]['_doGrouping'] = false;
                                                        }
                                                    }
                                                    draggableGrouping.removeGroupBy(c.id);
                                                    groupedByColumns = draggableGrouping.getColumnsGroupBy();
                                                    e.preventDefault();
                                                    e.stopPropagation();
                                                    m.redraw();
                                                }
                                            }),
                                            m(Icon, {
                                                class: "fas fa-arrow-square-up group-column-order",
                                                "data-tippy-content": "Re-order",
                                                "data-tippy-placement": "top",
                                                onclick: function (e) {
                                                    let c = getSelectedGridColumn(col.id);
                                                    reOrderGroups(c.id, true);
                                                    m.redraw();
                                                    e.preventDefault();
                                                    e.stopPropagation();
                                                }
                                            }),
                                            m(Icon, {
                                                class: "fas fa-arrow-square-down group-column-order",
                                                "data-tippy-content": "Re-order",
                                                "data-tippy-placement": "top",
                                                onclick: function (e) {
                                                    let c = getSelectedGridColumn(col.id);
                                                    reOrderGroups(c.id, false);
                                                    m.redraw();
                                                    e.preventDefault();
                                                    e.stopPropagation();
                                                }
                                            }),
                                        ])
                                    ]) : '';
                                })
                            ]),
                            m("div.dropdown-divider"),
                            m("div.slick-grid-header-group-filter-input", [
                                    m("div.input-group.text-muted", [
                                            m("input.form-control", {
                                                type: "text",
                                                placeholder: "Filter columns",
                                                value: columnsGroupFilterName,
                                                oninput: function (e) {
                                                    let tmpColumnsGroupFilterName = e.target.value;
                                                    if (columnsGroupFilterName && tmpColumnsGroupFilterName.includes(columnsGroupFilterName)) {
                                                        //its narrowing
                                                        columnsGroupFilterName = tmpColumnsGroupFilterName;
                                                        groupColumnsFilter(true);
                                                    } else {
                                                        //its expanding
                                                        columnsGroupFilterName = tmpColumnsGroupFilterName;
                                                        groupColumnsFilter(false);
                                                    }
                                                }
                                            }),
                                            m("span.input-group-text", m("i.fa.fa-times", {
                                                    class: columnsGroupFilterName !== "" ? "text-danger" : "",
                                                    onclick: function (e) {
                                                        columnsGroupFilterName = "";
                                                        groupColumnsFilter(false)
                                                        e.preventDefault();
                                                        e.stopPropagation();
                                                    }
                                                })
                                            )
                                        ]
                                    )
                                ]
                            ),
                            m("div.slick-grid-header-group-filter-columns", [
                                filteredGroupColumns.map(function (col) {
                                    return m("div.dropdown-item", [
                                        m("div.custom-dropdown-section", [
                                            m("input", {
                                                id: gridId + col.id,
                                                type: "checkbox",
                                                value: col.id,
                                                checked: col['_doGrouping'] === true,
                                                onclick: function (e, args) {
                                                    toggleGroupCheckboxSelection(col, this);
                                                }
                                            }),
                                            m("label", {for: gridId + col.id}, col.name)
                                        ])
                                    ]);
                                })
                            ]),
                        )
                    ])]
                ),
                m("span.slick-grid-header-icon",
                    m(Icon, {
                        id: gridId + "-group-toggle",
                        class: `fas ${groupsExpanded ? 'fa-expand-arrows-alt' : 'fa-compress-arrows-alt'}`,
                        "data-tippy-content": "Toggle collapse/expand",
                        "data-tippy-placement": "top",
                        onclick: function () {
                            if (groupsExpanded) {
                                dataView.expandAllGroups();
                            } else {
                                dataView.collapseAllGroups();
                            }
                            groupsExpanded = !groupsExpanded;
                            $("#" + gridId + "-group-reduction").dropdown('show');
                        }
                    })
                ),
                m("span.slick-grid-header-icon",
                    m(Icon, {
                        id: gridId + "-group-clear",
                        class: `fas fa-times`,
                        "data-tippy-content": "Clear groupings",
                        "data-tippy-placement": "top",
                        onclick: function () {
                            for (let i = 0; i < filteredGroupColumns.length; i++) {
                                let col = filteredGroupColumns[i];
                                let c = getSelectedGridColumnByIndex(i, col);
                                c['_doAggregate'] = false;
                                c['_doGrouping'] = false;
                                col['_doAggregate'] = false;
                                col['_doGrouping'] = false;
                            }
                            draggableGrouping.clearDroppedGroups();
                            groupedByColumns = draggableGrouping.getColumnsGroupBy();
                            m.redraw();
                        }
                    })
                ),
                m("span.slick-grid-header-icon", [
                    m("div.dropdown", [
                        m(Icon, {
                            id: gridId + "-group-reduction",
                            class: `fas fa-signature ${groupedByColumns.length > 0 ? 'dropdown-toggle' : 'text-muted'}`,
                            "data-tippy-content": "Choose aggregation",
                            "data-tippy-placement": "top",
                            attrs: {"data-toggle": "dropdown"},
                            "aria-haspopup": "true",
                            "aria-expanded": "false",
                            onclick: function (e) {
                                if (groupedByColumns.length === 0) {
                                    e.stopPropagation();
                                    e.preventDefault();
                                }
                            }
                        }),
                        m("div.dropdown-menu.dropdown-menu-sm.text-center", {
                                onclick: function (e) {
                                    e.stopPropagation();
                                }
                            },
                            columns.map(function (col) {
                                return m("div.dropdown-item", [
                                    m("div.custom-checkbox", [
                                        m("select.form-control.form-control-sm", {
                                            onclick: function (e) {
                                                let c = getSelectedGridColumn(col.id);
                                                c['_aggregation'] = e.target.value;
                                                aggregate(c);
                                                e.preventDefault();
                                                e.stopPropagation();
                                            }
                                        }, [
                                            m("option", "sum"),
                                            m("option", "avg"),
                                            m("option", "max"),
                                            m("option", "min"),
                                        ]),
                                        m("input", {
                                            id: gridId + col.id,
                                            type: "checkbox",
                                            value: col.id,
                                            checked: col['_doAggregate'] === true,
                                            onclick: function (e) {
                                                let c = getSelectedGridColumn(col.id);
                                                c['_doAggregate'] = this.checked;
                                                col['_doAggregate'] = this.checked;
                                                aggregate(c);
                                            }
                                        }),
                                        m("label", {for: gridId + col.id}, col.name)
                                    ]),
                                ]);
                            })
                        )
                    ])]
                ),
                m("div.slick-header-separator"),
                m("span.slick-grid-header-filter-column-input", [
                        m("div.input-group.text-muted", [
                                m("input.form-control", {
                                    type: "text",
                                    placeholder: "Filter columns",
                                    value: columnsFilter,
                                    oninput: function (e) {
                                        columnsFilter = e.currentTarget.value;
                                        filterColumns()
                                    }
                                }),
                                m("span.input-group-text", m("i.fa.fa-times", {
                                    class: columnsFilter !== "" ? "text-danger" : "",
                                    onclick: function () {
                                        columnsFilter = "";
                                        filterColumns()
                                    }
                                }))
                            ]
                        )
                    ]
                )
            ];
            //csv download
            let csvUri = "";
            if (gridTools.exportCsvFile !== undefined && gridTools.exportCsvFile !== null) {
                csvUri = encodeURIComponent(`cm/data/slickgrid/download/${gridTools.exportCsvFile.context}/${gridTools.exportCsvFile.filename}`);
                footerComponents.push(
                    m("span.slick-grid-footer-icon", {
                        title: "Csv download"
                    }, [
                        m("a", {
                            href: csvUri,
                            onclick: function (e) {
                                if (csvUri === "") {
                                    e.preventDefault();
                                    e.stopPropagation();
                                    e.redraw = false;
                                }
                            }
                        }, [
                            m('i', {
                                class: `fas fa-file-csv ${csvUri === '' ? 'text-muted' : ''}`,
                            })
                        ])
                    ])
                );
            }

            //xls download
            let xlsUri = ""
            if (gridTools.exportXlsFile !== undefined && gridTools.exportXlsFile !== null) {
                xlsUri = encodeURIComponent(`cm/data/slickgrid/download/${gridTools.exportXlsFile.context}/${gridTools.exportXlsFile.filename}`);
                footerComponents.push(
                    m("span.slick-grid-footer-icon", {
                        title: "Excel download"
                    }, [
                        m("a", {
                            href: xlsUri,
                            onclick: function (e) {
                                if (xlsUri === "") {
                                    e.preventDefault();
                                    e.stopPropagation();
                                    e.redraw = false;
                                }
                            }
                        }, [
                            m('i', {
                                class: `fas fa-file-excel ${xlsUri === '' ? 'text-muted' : ''}`,
                            })
                        ])
                    ])
                );
            } else {
                //UnFiltered download link
                footerComponents.push(
                    m("span.slick-grid-footer-icon", {
                        title: "Excel download"
                    }, [
                        m("a", {
                            href: '',
                            onclick: function (e) {
                                if (dataView && dataView.getItems().length > 0) {
                                    exportDataFromGrid(vnode.attrs.data.columns, false, gridId, gridId + ".xlsx");
                                } else {
                                    e.redraw = false;
                                }
                                e.preventDefault();
                                e.stopPropagation();
                            }
                        }, [
                            m('i', {
                                class: "fas fa-file-excel" + (gridTools.exportXlsFile && dataView && dataView.getItems().length === 0 ? " text-muted" : ""),
                            })
                        ])
                    ])
                );
            }

            //Filtered download link
            footerComponents.push(
                m("span.slick-grid-footer-icon", {
                    title: "Filtered Download"
                }, [
                    m("a", {
                        href: '',
                        onclick: function (e) {
                            if (gridTools.exportXlsFile) {
                                if (dataView && dataView.getFilteredItems().length < dataView.getItems().length) {
                                    preparingFilteredDownload = true;
                                    if (dataView.getFilteredItems().length > 1000) {
                                        exportFilteredRowsFromXlsxFile(gridTools.exportXlsFile.context, gridTools.exportXlsFile.filename);
                                    } else {
                                        let filename = gridTools.exportXlsFile.filename;
                                        let lastIndexOfDot = filename.lastIndexOf(".");
                                        let firstPart = filename.substring(0, lastIndexOfDot);
                                        let postFix = filename.substring(lastIndexOfDot);
                                        filename = firstPart + "_filtered" + postFix;
                                        exportDataFromGrid(vnode.attrs.data.columns, true, gridTools.exportXlsFile.context, filename);
                                    }
                                } else {
                                    e.redraw = false;
                                }
                            } else {
                                if (dataView && dataView.getItems().length > 0) {
                                    preparingFilteredDownload = true;
                                    if (dataView.getFilteredItems().length < dataView.getItems().length) {
                                        exportDataFromGrid(vnode.attrs.data.columns, true, gridId, gridId + "_filtered.xlsx");
                                    }
                                } else {
                                    e.redraw = false;
                                }
                            }
                            e.preventDefault();
                            e.stopPropagation();
                        }
                    }, [
                        m('i', {
                            class: "fas fa-file-download" + (dataView && dataView.getFilteredItems().length === dataView.getItems().length ? " text-muted" : ""),
                        })
                    ])
                ])
            );
            if (gridTools.exportFile && gridTools.exportFile.kml) {
                footerComponents.push(
                    m("span.slick-grid-footer-icon", {
                        title: "Kml download"
                    }, [
                        m("a", {href: `cm/data/slickgrid/download/${gridTools.exportFile.context}/${gridTools.exportFile.kml}`}, [
                            m('i', {
                                class: "fas fa-thumbtack",
                            })
                        ])
                    ])
                );
            }
            if (isTree === false && gridTools.showRows && data) {
                footerComponents.push(
                    m("span.ml-2", {class: "totalRows"},
                        filteredItemsLength === 0 ?
                            (data ? data.length + " total rows" : "") :
                            "Showing " + filteredItemsLength + " of " + data.length));
            }
            let spinnerOnly = vnode.attrs.showSpinner !== undefined || preparingFilteredDownload !== false;
            let isDownloading = vnode.attrs.downloadSize !== undefined && vnode.attrs.downloadSize !== 0;
            let gridClass = "gridContainer";
            if (vnode.attrs.hideGroupHeader === true && vnode.attrs.hideFooter === true) {
                gridClass += " hideGroupHeader hideFooter";
            } else if (vnode.attrs.hideGroupHeader === true) {
                gridClass += " hideGroupHeader";
            } else if (vnode.attrs.hideFooter === true) {
                gridClass += " hideFooter";
            }
            return m("div", {class: gridClass}, [
                vnode.attrs.hideGroupHeader === true ?
                    m("div", {key: gridId + "gridContainerHeader"}) :
                    m("div.slick-grid-tools.slick-grid-header-panel", {key: gridId + "gridContainerHeader"}, groupHeaderComponents),
                spinnerOnly ?
                    m("div.slick-grid-loader", {
                        key: 'slickGridSpinner',
                        class: ((vnode.attrs.showSpinner) ? "" : "d-none"),
                        style: {height: "100%", "z-index": 1}
                    }, [
                        m("div.slick-grid-spinner", [
                            m("i.fas", {class: isDownloading ? "downloading fa-spinner fa-spin" : "fa-spinner fa-spin"}),
                        ]),
                        m("div.text-muted.slick-grid-download-size", m("p", vnode.attrs.downloadSize !== 0 ? vnode.attrs.downloadSize : ""))
                    ]) :
                    m("div#countdown", {
                            key: 'slickGridCountDown',
                            class: ((vnode.attrs.spinner && vnode.attrs.spinner.show) ? "show-countdown" : "hide-countdown"),
                        }, [
                            m("div.countdown-inner",
                                m("div.slick-grid-countdown-number", {id: gridId + "-slick-grid-countdown-number"}, vnode.attrs.spinner === undefined ?
                                    "10" :
                                    (!vnode.attrs.spinner.downloadSize ? m("div.report-spinner-duration", vnode.attrs.spinner.duration) :
                                            m("div", [
                                                m("div.report-spinner-duration", vnode.attrs.spinner.duration),
                                                m("div.report-spinner-downloadsize", vnode.attrs.spinner.downloadSize)
                                            ])
                                    )
                                ),
                                m("svg", m("circle", {id: 'indicator-circle', r: "100", cx: "150", cy: "150"}))
                            ),
                        ]
                    ),
                m("div", {
                    key: "slickGrid",
                    id: gridId,
                    class: 'gridPane',
                    style: {height: (vnode.attrs.height ? vnode.attrs.height + "px" : '')}
                }),
                m(SlickDropdownFilter, {
                    id: gridId,
                    key: "slickGridDropdown",
                    colFilterString: selectedDropdownSlickFilterString,
                    popped: selectedDropdownSlickFilterActive,
                    colName: selectedDropdownSlickFilterColName,
                    selectedCmColumn: selectedCmColumn,
                    fireFilterEvent: fireFilterFromDropdownInputEvent
                }),
                vnode.attrs.data !== '' ? m('div.slick-grid-tools.d-flex.align-items-center', {
                        key: "slickGridTools",
                    },
                    [
                        vnode.attrs.hideFooter === true ?
                            m("div") :
                            m("div", footerComponents),
                        vnode.attrs.options !== undefined && vnode.attrs.options.legend !== undefined ?
                            m("div.float-right.text-muted.mr-2", [
                                m("small", "Legend: "),
                                vnode.attrs.options.legend.map(function (l) {
                                    return m("small.badge.badge-light.ml-2", {class: l.class}, l.label);
                                })
                            ]) :
                            m("div")
                    ]
                    ) :
                    m("div")
            ]);
        }
    }
}

export default SlickGrid2;