/**
 *
 * Draggable Grouping contributed by:  Muthukumar Selvarasu
 *  muthukumar{dot}se{at}gmail{dot}com
 *  github.com/muthukumarse/Slickgrid
 *
 * NOTES:
 *     This plugin provides the Draggable Grouping feature
 */

(function ($) {
    // Register namespace
    $.extend(true, window, {
        "Slick": {
            "DraggableGrouping": DraggableGrouping
        }
    });

    /***
     * A plugin to add Draggable Grouping feature.
     *
     * USAGE:
     *
     * Add the plugin .js & .css files and register it with the grid.
     *
     *
     * The plugin expose the following methods:
     *    destroy: used to destroy the plugin
     *    setDroppedGroups: provide option to set default grouping on loading
     *    clearDroppedGroups: provide option to clear grouping
     *    getSetupColumnReorder: its function to setup draggable feature agains Header Column, should be passed on grid option. Also possible to pass custom function
     *
     *
     * The plugin expose the following event(s):
     *    onGroupChanged: pass the grouped columns to who subscribed.
     *
     * @param options {Object} Options:
     *    deleteIconCssClass:  an extra CSS class to add to the delete button (default undefined), if deleteIconCssClass && deleteIconImage undefined then slick-groupby-remove-image class will be added
     *    deleteIconImage:     a url to the delete button image (default undefined)
     *    groupIconCssClass:   an extra CSS class to add to the grouping field hint  (default undefined)
     *    groupIconImage:      a url to the grouping field hint image (default undefined)
     *    dropPlaceHolderText:      option to specify set own placeholder note text
     *

     */

    function DraggableGrouping(options) {
        var _grid;
        var _gridUid;
        var _gridColumns;
        var _dataView;
        var dropbox;
        var dropboxPlaceholder;
        var groupToggler;
        var _defaults = {};
        var onGroupChanged = new Slick.Event();
        var _handler = new Slick.EventHandler();

        /**
         * Initialize plugin.
         */
        function init(grid) {
            options = $.extend(true, {}, _defaults, options);
            _grid = grid;
            _gridUid = _grid.getUID();
            _gridColumns = _grid.getColumns();
            _dataView = _grid.getData();

            dropbox = $(options.dropbox);
            var dropPlaceHolderText = options.dropPlaceHolderText || 'Drop a column header here to group by the column';
            dropbox.html(`<div class='slick-placeholder text-muted'>${dropPlaceHolderText}</div>`);
            dropboxPlaceholder = dropbox.find(".slick-placeholder");
            groupToggler = dropbox.find(".slick-group-toggle-all");
            setupColumnDropbox();

            _handler.subscribe(_grid.onHeaderCellRendered, function (e, args) {
                var column = args.column;
                var node = args.node;
                if (!$.isEmptyObject(column.grouping)) {
                    var groupableIcon = $("<span class='slick-column-groupable' />");
                    if (options.groupIconCssClass) {
                        groupableIcon.addClass(options.groupIconCssClass);
                    }
                    if (options.groupIconImage) {
                        groupableIcon.css("background", "url(" + options.groupIconImage + ") no-repeat center center");
                    }
                    $(node).css('cursor', 'pointer').append(groupableIcon);
                }
            });

            for (var i = 0; i < _gridColumns.length; i++) {
                var columnId = _gridColumns[i].field;
                _grid.updateColumnHeader(columnId);
            }

        }

        function setupColumnReorder(grid, $headers, headerColumnWidthDiff, setColumns, setupColumnResize, columns, getColumnIndex, uid, trigger) {
            $headers.filter(":ui-sortable").sortable("destroy");
            // var $headerDraggableGroupBy = $(grid.getPreHeaderPanel());
            let $headerDraggableGroupBy = $(options.dropbox);
            $headers.sortable({
                distance: 3,
                cursor: "default",
                tolerance: "intersection",
                helper: "clone",
                placeholder: "slick-sortable-placeholder ui-state-default slick-header-column",
                forcePlaceholderSize: true,
                appendTo: "body",
                start: function (e, ui) {
                    $(ui.helper).addClass("slick-header-column-active");
                    dropboxPlaceholder.show();
                    // $headerDraggableGroupBy.find(".slick-placeholder").show();
                    $headerDraggableGroupBy.find(".slick-grid-dropped-grouping").hide();
                },
                beforeStop: function (e, ui) {
                    $(ui.helper).removeClass("slick-header-column-active");
                    let hasDroppedColumn = $headerDraggableGroupBy.find(".slick-grid-dropped-grouping").length;
                    if (hasDroppedColumn > 0) {
                        dropboxPlaceholder.hide();
                        // $headerDraggableGroupBy.find(".slick-placeholder").hide();
                        $headerDraggableGroupBy.find(".slick-grid-dropped-grouping").show();
                    }
                },
                stop: function (e) {
                    if (!grid.getEditorLock().commitCurrentEdit()) {
                        $(this).sortable("cancel");
                        return;
                    }
                    var reorderedIds = $headers.sortable("toArray");
                    var reorderedColumns = [];
                    for (var i = 0; i < reorderedIds.length; i++) {
                        reorderedColumns.push(columns[getColumnIndex(reorderedIds[i].replace(uid, ""))]);
                    }
                    setColumns(reorderedColumns);
                    trigger(grid.onColumnsReordered, {
                        grid: grid
                    });
                    e.stopPropagation();
                    setupColumnResize();
                }
            });
        }

        /**
         * Destroy plugin.
         */
        function destroy() {
            onGroupChanged.unsubscribe();
            _handler.unsubscribeAll();
        }


        function setColumns(col) {
            _gridColumns = col;
        }

        var emptyDropbox;

        function setupColumnDropbox() {
            dropbox.droppable({
                activeClass: "bg-cm-light-blue",
                // activeClass: "ui-state-default",
                // hoverClass: "ui-state-hover",
                hoverClass: "bg-cm-selected-blue",
                accept: ":not(.ui-sortable-helper)",
                deactivate: function (event, ui) {
                    dropbox.removeClass("slick-header-column-denied");
                },
                drop: function (event, ui) {
                    handleGroupByDrop(this, ui.draggable);
                },
                over: function (event, ui) {
                    let id = (ui.draggable).attr('id').replace(_gridUid, "");
                    _gridColumns.forEach(function (e, i, a) {
                        if (e.id === id) {
                            if (e.grouping == null) {
                                dropbox.addClass("slick-header-column-denied");
                            }
                        }
                    });
                }
            });
            dropbox.sortable({
                items: "div.slick-grid-dropped-grouping",
                cursor: "default",
                tolerance: "pointer",
                helper: "clone",
                update: function (event, ui) {
                    let sortArray = $(this).sortable('toArray', {
                            attribute: 'data-id'
                        }),
                        newGroupingOrder = [];
                    for (let i = 0, l = sortArray.length; i < l; i++) {
                        for (let a = 0, b = columnsGroupBy.length; a < b; a++) {
                            if (columnsGroupBy[a].id === sortArray[i]) {
                                newGroupingOrder.push(columnsGroupBy[a]);
                                break;
                            }
                        }
                    }
                    columnsGroupBy = newGroupingOrder;
                    updateGroupBy("sort-group");
                }
            });
            emptyDropbox = dropbox.html();

            groupToggler.on('click', function (e) {
                if (this.classList.contains('collapsed')) {
                    this.classList.remove('collapsed');
                    this.classList.add('expanded');
                    _dataView.expandAllGroups();
                } else {
                    this.classList.add('collapsed');
                    this.classList.remove('expanded');
                    _dataView.collapseAllGroups();
                }
            });
        }

        var columnsGroupBy = [];
        var groupBySorters = [];

        function handleGroupBySelection(sortedArray) {
            let newGroupingSelectionOrder = [];
            for (let i = 0, l = sortedArray.length; i < l; i++) {
                for (let a = 0, b = columnsGroupBy.length; a < b; a++) {
                    if (columnsGroupBy[a].id === sortedArray[i].id) {
                        newGroupingSelectionOrder.push(columnsGroupBy[a]);
                        break;
                    }
                }
            }
            columnsGroupBy = newGroupingSelectionOrder;
            updateGroupBy("sort-group");
        }

        function handleGroupByDrop(container, column) {
            let columnId = column.attr('id').replace(_gridUid, "");
            let columnAllowed = true;
            for (let i = 0; i < columnsGroupBy.length; i++) {
                let columnGroupBy = columnsGroupBy[i];
                if (columnGroupBy.id === columnId) {
                    columnAllowed = false;
                }
            }
            if (columnAllowed) {
                for (let i = 0; i < _gridColumns.length; i++) {
                    let col = _gridColumns[i];
                    if (col.id === columnId) {
                        createGroupedEntry(container, col);
                        addColumnGroupBy(col);
                    }
                }
                groupToggler.css('display', 'block');
            }
        }

        function createGroupedEntry(dropBox, col) {
            let entry = $("<div id='" + _gridUid + col.id + "_entry' data-id='" + col.id + "' class='slick-grid-dropped-grouping badge d-flex text-muted'>");
            let groupText = $("<div style='display: inline-flex'>" + col.name + "</div>");
            groupText.appendTo(entry);
            let groupRemoveIcon = $("<i class='pl-2 fas fa-times'></i>");
            groupRemoveIcon.on("click", function (event) {
                $(this).off('click');
                removeGroupBy(col.id, entry);
            });
            groupRemoveIcon.appendTo(entry);
            $("</div>").appendTo(entry);
            entry.appendTo(dropBox);
            dropboxPlaceholder.hide();
        }

        function addColumnGroupBy(column) {
            column["header"]["buttons"].forEach(function (b) {
                if (b.command === "groupby-column") {
                    b.cssClass = "fas fa-bars slick-header-group-column-active text-primary";
                }
            });
            _grid.updateColumnHeader(column.field);
            columnsGroupBy.push(column);
            updateGroupBy("add-group");
        }

        function getColumnsGroupBy() {
            return columnsGroupBy;
        }

        function addGroupByRemoveClickHandler(id, container, column, entry) {
            var text = entry;
            $("#" + _gridUid + id + "_entry >.slick-groupby-remove").on('click', function () {
                $(this).off('click');
                removeGroupBy(id, text);
            });
        }

        function setDroppedGroups(groupingInfo) {
            var groupingInfos = (groupingInfo instanceof Array) ? groupingInfo : [groupingInfo];
            dropboxPlaceholder.hide();
            for (var i = 0; i < groupingInfos.length; i++) {
                var column = $(_grid.getHeaderColumn(groupingInfos[i]));
                handleGroupByDrop(dropbox, column);
            }
        }

        function clearDroppedGroups() {
            columnsGroupBy.forEach(function (col) {
                col["header"]["buttons"].forEach(function (b) {
                    if (b.command === "groupby-column") {
                        b.cssClass = "fas fa-bars";
                        _grid.updateColumnHeader(col.field);
                    }
                });
            });
            columnsGroupBy = [];
            updateGroupBy("clear-all");
            dropbox.find(".slick-grid-dropped-grouping").remove();
            groupToggler.css("display", "none");
            dropboxPlaceholder.show();
        }

        function removeGroupBy(id, entry) {
            if (typeof entry !== "undefined") {
                entry.remove();
            } else if ($("#" + _gridUid + id + "_entry")) {
                $("#" + _gridUid + id + "_entry").remove();
            }
            let i = columnsGroupBy.length;
            while (i--) {
                if (columnsGroupBy[i].id === id) {
                    columnsGroupBy[i]["header"]["buttons"].forEach(function (b) {
                        if (b.command === "groupby-column") {
                            b.cssClass = "fas fa-bars";
                        }
                    });
                    _grid.updateColumnHeader(columnsGroupBy[i].field);
                    columnsGroupBy.splice(i, 1);
                    break;
                }
            }
            if (columnsGroupBy.length === 0) {
                dropboxPlaceholder.show();
            }
            updateGroupBy("remove-group");
        }

        function addReductionToGrouping(column) {
            let groupingArray = _dataView.getGrouping();
            let groupingAsMap = groupingArray.reduce(function (map, obj) {
                map[obj.getter] = obj;
                return map;
            }, {});
            for (let i = 0; i < columnsGroupBy.length; i++) {
                let columnGroupBy = columnsGroupBy[i];
                for (let j = 0; j < column.aggregators.length; j++) {
                    let aggregator = column.aggregators[j];
                    groupingAsMap[columnGroupBy.field].aggregators.push(aggregator);
                }
            }
            _dataView.setGrouping(groupingArray);
        }

        function removeReductionToGrouping(column) {
            let groupingArray = _dataView.getGrouping();
            let groupingAsMap = groupingArray.reduce(function (map, obj) {
                map[obj.getter] = obj;
                return map;
            }, {});
            for (let i = 0; i < columnsGroupBy.length; i++) {
                let columnGroupBy = columnsGroupBy[i];
                let j = groupingAsMap[columnGroupBy.field].aggregators.length
                while (j--) {
                    let aggregator = groupingAsMap[columnGroupBy.field].aggregators[j];
                    for (let k = 0; k < column.aggregators.length; k++) {
                        let columnAggregator = column.aggregators[k];
                        if (aggregator.field_ === columnAggregator.field_) {
                            //remove
                            groupingAsMap[columnGroupBy.field].aggregators.splice(j, 1);
                        }
                    }
                }
            }
            _dataView.setGrouping(groupingArray);
        }

        function updateGroupBy(originator) {
            if (columnsGroupBy.length === 0) {
                _dataView.setGrouping([]);
                onGroupChanged.notify({caller: originator, groupColumns: []});
                return;
            }
            let groupingArray = [];
            columnsGroupBy.forEach(function (element, index, array) {
                groupingArray.push({
                    getter: element.field,
                    formatter: function (g) {
                        return `<b>${element.name}</b>: ${g.value} <i>${g.count} ${(g.count !== 1 ? "items" : "item")}</i>`;
                    },
                    aggregateCollapsed: false,
                    lazyTotalsCalculation: true
                });
            });
            _dataView.setGrouping(groupingArray);
            onGroupChanged.notify({caller: originator, groupColumns: groupingArray});
        }


        // Public API
        $.extend(this, {
            "init": init,
            "destroy": destroy,
            "pluginName": "DraggableGrouping",

            "onGroupChanged": onGroupChanged,
            "setDroppedGroups": setDroppedGroups,
            "getColumnsGroupBy": getColumnsGroupBy,
            "clearDroppedGroups": clearDroppedGroups,
            "getSetupColumnReorder": setupColumnReorder,
            "addReductionToGrouping": addReductionToGrouping,
            "removeReductionToGrouping": removeReductionToGrouping,
            "createGroupedEntry": createGroupedEntry,
            "addColumnGroupBy": addColumnGroupBy,
            "removeGroupBy": removeGroupBy,
            "handleGroupBySelection": handleGroupBySelection
        });
    }
})(jQuery);
