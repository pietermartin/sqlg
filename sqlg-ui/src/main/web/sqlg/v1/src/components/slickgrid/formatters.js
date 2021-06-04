import tippy from "tippy.js";

// noinspection JSUnusedLocalSymbols
const Formatters = {

    //Formatter Specific to Comparator
    TargetNodeFormatter: function (row, cell, value, columnDef, dataContext) {
        value = value
            .replace(/&/g, "&amp;")
            .replace(/</g, "&lt;")
            .replace(/>/g, "&gt;");
        if (typeof dataContext.left && typeof dataContext[columnDef.id]) {
            if (row > 2 && dataContext.left !== dataContext[columnDef.id]) {
                if (dataContext.left === '' && !dataContext.isUid || dataContext.left === null && !dataContext.isUid) {
                    return '<div class="cell-target-single-node">' + value + '</div>';
                } else if (dataContext.left === '' && dataContext.isUid || dataContext.left === null && dataContext.isUid) {
                    return '<div class="cell-target-uid-single-node">' + value + '</div>';
                }
                if (!dataContext.isUid) {
                    return '<div class="cell-target-mismatch">' + value + '</div>';
                } else {
                    return '<div class="cell-target-uid-mismatch">' + value + '</div>';
                }
            } else {
                return '<div class="cell-target-value">' + value + '</div>';
            }
        } else {
            return '<div class="cell-target-value">' + value + '</div>';
        }
    },

    NumberFormatter: function (row, cell, value, columnDef, dataContext) {
        return `<div style='text-align: right'>${value}</div>`;
    },

    SumTotalsFormatter: function(totals, columnDef) {
        var val = totals.sum && totals.sum[columnDef.field];
        if (val != null) {
            return "total: " + ((Math.round(parseFloat(val)*100)/100));
        }
        return "";
    },

    AvgTotalsFormatter: function(totals, columnDef) {
        var val = totals.avg && totals.avg[columnDef.field];
        if (val != null) {
            return "average: " + ((Math.round(parseFloat(val)*100)/100));
        }
        return "";
    },

    MinTotalsFormatter: function(totals, columnDef) {
        var val = totals.min && totals.min[columnDef.field];
        if (val != null) {
            return "min: " + ((Math.round(parseFloat(val)*100)/100));
        }
        return "";
    },

    MaxTotalsFormatter: function(totals, columnDef) {
        var val = totals.max && totals.max[columnDef.field];
        if (val != null) {
            return "max: " + ((Math.round(parseFloat(val)*100)/100));
        }
        return "";
    },

    LocalDateTimeFormatter: function (row, cell, value, columnDef, dataContext) {
        return `<div style='text-align: left'>${value}</div>`;
    },

    LocalDateFormatter: function (row, cell, value, columnDef, dataContext) {
        return `<div style='text-align: left'>${value}</div>`;
    },

    LocalTimeFormatter: function (row, cell, value, columnDef, dataContext) {
        return `<div style='text-align: left'>${value}</div>`;
    },

    DeletionFormatter: function (row, cell, value, columnDef, dataContext) {
        return "<div style='text-align: center'><i class='fas fa-trash-alt'></i></div>";
    },

    SelectRowFormatter: function (row, cell, value, columnDef, dataContext) {
        return `<div style="text-align: center"><i class='far ${dataContext.selected ? "fa-check-square" : "fa-square"}'></i></div>`;
    },

    CheckboxFormatter: function (row, cell, value, columnDef, dataContext) {
        return `<div style="text-align: center"><i class='fas ${value === true || value === 'true' ? "fa-check" : "fa-times"}'></i></div>`;
    },

    RequiredFormatter(row, cell, value, columnDef, dataContext) {
        let rtn = {text: value};
        if (value === null || value === undefined || !value.length) {
            rtn.addClasses = "invalid";
        } else {
            rtn.removeClasses = "invalid";
        }
        return rtn;
    },

    TreeFormatter (row, cell, value, columnDef, dataContext) {
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
        value = escapeHtml(value);
        let spacer = "<span style='display:inline-block;height:1px;width:" + (15 * dataContext["level"]) + "px'></span>";
        let retval;
        if (dataContext._collapsed) {
            retval = `${spacer}<i class='${dataContext.isLeaf ? "far fa-leaf" : "fas fa-caret-right toggle expand"}' aria-hidden='true'></i>`;
        } else {
            retval = `${spacer}<i class='${dataContext.isLeaf ? "far fa-leaf" : "fas fa-caret-down toggle collapse"}' aria-hidden='true'></i>`;
        }
        if (dataContext.isUid) {
            retval += "<i class='fas fa-key'></i>";
        }
        retval += "<span class='slick-cell-show-all'>" + value;
        retval += "</span>";
        return retval;
    },

    SelectCellFormatter: function(row, cell, value, columnDef, dataContext) {
        let ret = columnDef.options.filter(function(a){return a.id == value});
        if (ret.length > 0) {
            return ret[0].name;
        } else {
            return null;
        }

    }

};

export default Formatters;