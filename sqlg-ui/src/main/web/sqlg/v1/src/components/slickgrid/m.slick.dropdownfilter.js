import m from "mithril";
import $ from "jquery";
import Input from "../form/input";
import Button from "../form/button";
import _ from "underscore";
import Utils from "../../utils/utils";
import ImportButton from "../form/importButton";

function SlickDropdownFilter() {

    let gridId = undefined;
    let isImportingFilter = false;
    let filterFunction;
    let importFilterFunction;
    let tmpColumnInput;

    let isNumberFilter = function (columnType) {
        return columnType === 'SHORT' ||
            columnType === 'INTEGER' ||
            columnType === 'LONG' ||
            columnType === 'FLOAT' ||
            columnType === 'DOUBLE';
    };

    let isStringFilter = function (columnType) {
        return columnType === 'STRING' ||
            columnType === 'STRING_ARRAY';
    };

    let parseCsvDataFromDisk = function (data) {
        let parsedata = [];
        let returnString = '';
        let newLinebrk = data.split("\n");
        for (let i = 0; i < newLinebrk.length; i++) {
            parsedata.push(newLinebrk[i].split(","))
        }
        parsedata.forEach(function (arr) {
            if (arr[0] !== '') {
                returnString += arr[0] + ',';
            }
        });
        return returnString.slice(0, -1);
    };

    let readCsvFromDisk = function (e, callBack, callBackError) {
        return new Promise(function (resolve, reject) {
            if (e.target.files && e.target.files[0]) {
                let myFile = e.target.files[0];
                let reader = new FileReader();
                reader.addEventListener('load', function (e) {
                    let csvdata = e.target.result;
                    resolve(parseCsvDataFromDisk(csvdata));
                });
                reader.readAsBinaryString(myFile);
            }
        });
    };

    let constructFilterString = function (inputString, jqueryCol) {

        if (inputString === undefined) {
            return;
        }

        let outputString = '';

        switch (filterFunction) {
            case Utils.SEARCH_ENUM.contains:
                outputString = `x.includes('${inputString}')`;
                break;
            case Utils.SEARCH_ENUM.containsCIS:
                outputString = `x.toUpperCase().includes('${inputString}'.toUpperCase())`;
                break;
            case Utils.SEARCH_ENUM.ncontains:
                outputString = `!x.includes('${inputString}')`;
                break;
            case Utils.SEARCH_ENUM.ncontainsCIS:
                outputString = `!x.toUpperCase().includes('${inputString}'.toUpperCase())`;
                break;
            case Utils.SEARCH_ENUM.startsWith:
                outputString = `x.startsWith('${inputString}')`;
                break;
            case Utils.SEARCH_ENUM.endsWith:
                outputString = `x.endsWith('${inputString}')`;
                break;
            case Utils.SEARCH_ENUM.equals:
                outputString = `x === ${inputString}`;
                break;
            case Utils.SEARCH_ENUM.nequals:
                outputString = `x !== ${inputString}`;
                break;
            case Utils.SEARCH_ENUM.greater:
                outputString = `x > ${inputString}`;
                break;
            case Utils.SEARCH_ENUM.greaterEqual:
                outputString = `x >= ${inputString}`;
                break;
            case Utils.SEARCH_ENUM.small:
                outputString = `x < ${inputString}`;
                break;
            case Utils.SEARCH_ENUM.smallEqual:
                outputString = `x <= ${inputString}`;
        }
        jqueryCol.children('.fa-filter').attr('data-column-filter-string', outputString);
        return outputString;
    };

    let getColumnInput = function (string, type) {
        let result = '';
        let tmpString = '';
        try {
            if (type === 'STRING') {
                tmpString = string.replaceAll('\'', '').replaceAll("\"", "");
                result = tmpString.match(/\(([^)]+)\)/)[1];
            } else if (type === 'NUMBER') {
                result = string.replace(/[^0-9.\-]/g, '');
            }
        } catch (e) {
            console.log('expression failed.');
            result = '';
        }
        return result;
    };

    let deConstructFilterString = function (filterString) {
        //For Strings
        let tmpFilterString = filterString.substring(0, filterString.indexOf("("));
        //For Number logic
        let tmpFilterStringNumbers = function (string) {
            let strArray = string.split(' ');
            let logic = strArray.slice(0, 2).join(' ')
            return logic;
        };

        if (filterString === undefined || filterString === '') {
            return;
        }

        switch (true) {
            case tmpFilterString === 'x.includes':
                filterFunction = Utils.SEARCH_ENUM.contains;
                tmpColumnInput = getColumnInput(filterString, 'STRING');
                break;
            case tmpFilterString === 'x.toUpperCase().includes':
                filterFunction = Utils.SEARCH_ENUM.containsCIS;
                tmpColumnInput = getColumnInput(filterString, 'STRING');
                break;
            case tmpFilterString === '!x.includes':
                filterFunction = Utils.SEARCH_ENUM.ncontains;
                tmpColumnInput = getColumnInput(filterString, 'STRING');
                break;
            case tmpFilterString === '!x.toUpperCase().includes':
                filterFunction = Utils.SEARCH_ENUM.ncontainsCIS;
                tmpColumnInput = getColumnInput(filterString, 'STRING');
                break;
            case tmpFilterString === 'x.startsWith':
                filterFunction = Utils.SEARCH_ENUM.startsWith;
                tmpColumnInput = getColumnInput(filterString, 'STRING');
                break;
            case tmpFilterString === 'x.endsWith':
                filterFunction = Utils.SEARCH_ENUM.endsWith;
                tmpColumnInput = getColumnInput(filterString, 'STRING');
                break;
            case tmpFilterStringNumbers(filterString) === 'x ===':
                filterFunction = Utils.SEARCH_ENUM.equals;
                tmpColumnInput = getColumnInput(filterString, 'NUMBER');
                break;
            case tmpFilterStringNumbers(filterString) === 'x !==':
                filterFunction = Utils.SEARCH_ENUM.nequals;
                tmpColumnInput = getColumnInput(filterString, 'NUMBER');
                break;
            case tmpFilterStringNumbers(filterString) === 'x >':
                filterFunction = Utils.SEARCH_ENUM.greater;
                tmpColumnInput = getColumnInput(filterString, 'NUMBER');
                break;
            case tmpFilterStringNumbers(filterString) === 'x >=':
                filterFunction = Utils.SEARCH_ENUM.greaterEqual;
                tmpColumnInput = getColumnInput(filterString, 'NUMBER');
                break;
            case tmpFilterStringNumbers(filterString) === 'x <':
                filterFunction = Utils.SEARCH_ENUM.small;
                tmpColumnInput = getColumnInput(filterString, 'NUMBER');
                break;
            case tmpFilterStringNumbers(filterString) === 'x <=':
                filterFunction = Utils.SEARCH_ENUM.smallEqual;
                tmpColumnInput = getColumnInput(filterString, 'NUMBER');
        }
        m.redraw();
    };

    let searchEnabled = function () {
        return tmpColumnInput !== "" && tmpColumnInput !== undefined;
    };
    let clearAndClose = function (vnode) {
        filterFunction = undefined;
        tmpColumnInput = undefined;
        vnode.attrs.colSetFilterActive('__RESET_ALL__');
        vnode.attrs.fireFilterEvent(
            constructFilterString("", vnode.attrs.jqueryCol),
            vnode.attrs.colName
        );
        $(`#SlickDropdownFilter_${gridId}`).hide();
    };

    let search = function (vnode) {
        vnode.attrs.fireFilterEvent(
            constructFilterString(tmpColumnInput, vnode.attrs.jqueryCol),
            vnode.attrs.colName
        );
        vnode.attrs.colSetFilterActive('__RESET_ALL__');
        $(`#SlickDropdownFilter_${gridId}`).hide();
    }

    return {
        oninit: function (vnode) {
            gridId = vnode.attrs.id;
        },
        oncreate: function (vnode) {
            $(document).on('click.' + "SlickDropdownFilter_" + gridId, function (event) {
                if ($(event.target).closest("#SlickDropdownFilter_" + gridId).length === 0) {
                    let dropdowns = $("#SlickDropdownFilter_" + gridId);
                    vnode.attrs.colSetFilterActive('__RESET_ALL__');
                    dropdowns.hide();
                }
            });
            $(document).on('keyup.' + "SlickDropdownFilter_" + gridId, function (e) {
                if (e.keyCode === 27) {
                    let dropdowns = $("#SlickDropdownFilter_" + gridId);
                    vnode.attrs.colSetFilterActive('__RESET_ALL__');
                    dropdowns.hide();
                }
            });
        },
        onremove: function () {
            $(document).off("click.SlickDropdownFilter_" + gridId);
            $(document).off("keyup.SlickDropdownFilter_" + gridId);
        },
        view: function (vnode) {
            if (filterFunction === undefined) {
                if (isStringFilter(vnode.attrs.colType)) {
                    filterFunction = Utils.SEARCH_ENUM.contains;
                } else if (isNumberFilter(vnode.attrs.colType)) {
                    filterFunction = Utils.SEARCH_ENUM.equals;
                } else {
                    filterFunction = Utils.SEARCH_ENUM.contains;
                }
            }
            if (importFilterFunction === undefined) {
                if (isStringFilter(vnode.attrs.colType)) {
                    importFilterFunction = Utils.SEARCH_ENUM.contains;
                } else if (isNumberFilter(vnode.attrs.colType)) {
                    importFilterFunction = Utils.SEARCH_ENUM.equals;
                } else {
                    importFilterFunction = Utils.SEARCH_ENUM.contains;
                }
            }

            if (vnode.attrs.colFilterString !== undefined && vnode.attrs.colFilterString !== '' && vnode.attrs.colGetFilterActive) {
                deConstructFilterString(vnode.attrs.colFilterString);
                vnode.attrs.colSetFilterActive('__RESET_ALL__');
            }

            return m(`div#SlickDropdownFilter_${gridId}`, {class: `dropdown-menu SlickDropdownFilter bg-light`}, [
                m('div#SlickDropdownFilterContainer', [
                    m('div.dropdown-item', [
                        m("div.input-group", [
                            m(Input, {
                                id: "searchInput_" + gridId,
                                placeHolder: 'Filter value',
                                enabled: true,
                                value: tmpColumnInput,
                                // type: 'number',
                                // pattern: "^[0-9]",
                                attrs: {min: 0, step: 100},
                                oninput: function (e) {
                                    tmpColumnInput = e.target.value;
                                },
                                onkeyup: function (e) {
                                    if (e.keyCode === 13) {
                                        search(vnode);
                                    }
                                }
                            }),
                            m("div.input-group-append", [
                                m(Button, {
                                    id: "filterButton_" + gridId,
                                    class: "btn-sm btn-outline-success",
                                    text: "",
                                    icon: filterFunction.icon,
                                    iconOnTheLeft: false,
                                    'data-tippy-content': "Search '" + filterFunction.description + "'",
                                    'data-tippy-placement': "top",
                                    enabled: searchEnabled(),
                                    onclick: function () {
                                        search(vnode);
                                    },
                                }),
                                m(Button, {
                                    id: "filterDropDownButton_" + gridId,
                                    class: "btn-sm btn-outline-success dropdown-toggle dropdown-toggle-split",
                                    enabled: searchEnabled(),
                                    attrs: {"data-toggle": "dropdown", "data-boundary": "window"}
                                }),
                                m("div.dropdown-menu.dropdown-menu-right", [
                                        _.keys(Utils.SEARCH_ENUM).map(function (searchName) {
                                            let searchEnum = Utils.SEARCH_ENUM[searchName];
                                            return m(Button, {
                                                class: "dropdown-item",
                                                text: searchEnum.description,
                                                icon: searchEnum.icon,
                                                onclick: function () {
                                                    filterFunction = searchEnum;
                                                }
                                            })
                                        })
                                    ]
                                )
                            ])
                        ])
                    ]),
                    m("div.filter-button-panel.ml-4.mr-4", [
                            m("div.btn-group",
                                m(ImportButton, {
                                    text: "Import csv",
                                    id: "csvFilterImport_" + gridId,
                                    enabled: true,
                                    class: "btn-primary btn-sm",
                                    icon: "fas fa-plus",
                                    onchange: function (e) {
                                        if (isImportingFilter) {
                                            e.preventDefault();
                                            return;
                                        }
                                        isImportingFilter = true;

                                        readCsvFromDisk(e).then(function (data) {
                                            let arr = data.split(',');
                                            let filteredArr = arr.filter(function (el) {
                                                return el != null && el !== "";
                                            });
                                            let searchFunctionContains = 'return ' + '[' + filteredArr.map(function (i) {
                                                return "\"" + i.toString().toLowerCase() + "\"";
                                            }) + ']' + '.some(function(arrayItem){return x.toLowerCase().indexOf(arrayItem.toLowerCase()) > -1;},{x:x});'
                                            let searchFunctionEquals = 'return ' + '[' + filteredArr.map(function (i) {
                                                return "\"" + i.toString() + "\"";
                                            }) + ']' + '.some(function(arrayItem){return x === arrayItem;},{x:x});'

                                            vnode.attrs.fireFilterEvent(
                                                searchFunctionEquals,
                                                vnode.attrs.colName
                                            );
                                            vnode.attrs.colSetFilterActive('__RESET_ALL__');
                                            $(`#SlickDropdownFilter_${gridId}`).hide();
                                            isImportingFilter = false;
                                        });
                                    }
                                }),
                                m(Button, {
                                    id: "importFilterIconButton_" + gridId,
                                    class: "btn-sm btn-info import-filter-icon-button",
                                    icon: importFilterFunction.icon
                                }),
                                m(Button, {
                                    id: "importFilterDropDownButton_" + gridId,
                                    class: "btn-sm btn-info dropdown-toggle dropdown-toggle-split",
                                    enabled: true,
                                    attrs: {"data-toggle": "dropdown", "data-boundary": "window"}
                                }),
                                m("div.dropdown-menu.dropdown-menu-right", [
                                        _.keys(Utils.SEARCH_ENUM).map(function (searchName) {
                                            let searchEnum = Utils.SEARCH_ENUM[searchName];
                                            return m(Button, {
                                                class: "dropdown-item",
                                                text: searchEnum.description,
                                                icon: searchEnum.icon,
                                                onclick: function () {
                                                    importFilterFunction = searchEnum;
                                                }
                                            })
                                        })
                                    ]
                                )
                            ),
                            m(Button, {
                                class: "btn-danger btn-sm pr-4 float-right",
                                text: "Close",
                                icon: "fa-undo",
                                onclick: function () {
                                    clearAndClose(vnode);
                                }
                            })
                        ]
                    )
                ])
            ])
        }
    }
}

export default SlickDropdownFilter;