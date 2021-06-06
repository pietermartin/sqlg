import m from "mithril";
import $ from "jquery";
import Input from "../form/input";
import Button from "../form/button";
import _ from "underscore";
import Utils from "../../utils/utils";
import ImportButton from "../form/importButton";
import SlickGridConstants from "./slickGridConstants";
import Switch from "../switch/switch";
import Label from "../form/label";
import CmMithrilGlobal from "../../utils/CmMithrilGlobal";

function SlickDropdownFilter() {

    let gridId = undefined;
    let visible = false;
    let isImportingFilter = false;
    let currentGridColumnFilterString;
    let cmColumn;
    let columnName;
    let columnType = SlickGridConstants.COLUMN_TYPE.STRING;
    let fireFilterEventCallBack;
    let searchFields = [];

    let initSearchFields = () => {
        searchFields = [{
            searchEnum: columnType.defaultSearchEnum,
            importSearchEnum: columnType.defaultSearchEnum,
            columnInput: undefined,
            columnFilterString: undefined,
            isArray: false,
            isAnd: false
        }];
    };

    let resetSearchFields = () => {
        searchFields.splice(0, searchFields.length);
        searchFields.push({
            searchEnum: columnType.defaultSearchEnum,
            importSearchEnum: columnType.defaultSearchEnum,
            columnInput: undefined,
            columnFilterString: undefined,
            isArray: false,
            isAnd: false
        });
    };

    let parseCsvDataFromDisk = (data) => {
        let parsedata = [];
        let returnString = '';
        let newLinebrk = data.split("\n");
        for (let i = 0; i < newLinebrk.length; i++) {
            parsedata.push(newLinebrk[i].split(","))
        }
        parsedata.forEach((arr) => {
            if (arr[0] !== '') {
                returnString += arr[0] + ',';
            }
        });
        return returnString.slice(0, -1);
    };

    let readCsvFromDisk = (e) => {
        return new Promise(function (resolve, reject) {
            if (e.target.files && e.target.files[0]) {
                let myFile = e.target.files[0];
                let reader = new FileReader();
                reader.addEventListener('load', (e) => {
                    let csvdata = e.target.result;
                    resolve(parseCsvDataFromDisk(csvdata));
                });
                reader.readAsBinaryString(myFile);
            }
        });
    };

    let constructFilterString = () => {
        let outputString = "";
        let filteredArr = [];
        for (let i = 0; i < searchFields.length; i++) {
            let searchItem = searchFields[i];
            let inputString = searchItem.columnInput;
            if (i !== 0) {
                outputString += searchItem.isAnd ? " && " : " || ";
            }
            if (searchItem.isArray) {
                let arr = inputString.split(',');
                filteredArr = arr.filter((el) => {
                    return el != null && el !== "";
                });
            }
            switch (searchItem.searchEnum) {
                case Utils.SEARCH_ENUM.contains:
                    if (searchItem.isArray) {
                        outputString += '([' + filteredArr.map((i) => {
                            return "\'" + i.toString() + "\'";
                        }) + ']' + '.some(function(arrayItem){return x.includes(arrayItem);},{x:x}))';
                    } else {
                        outputString += `(x.includes('${inputString}'))`;
                    }
                    break;
                case Utils.SEARCH_ENUM.containsCIS:
                    if (searchItem.isArray) {
                        outputString += '([' + filteredArr.map((i) => {
                            return "\'" + i.toString() + "\'";
                        }) + ']' + '.some(function(arrayItem){return x.toUpperCase().includes(arrayItem.toUpperCase());},{x:x}))';
                    } else {
                        outputString += `(x.toUpperCase().includes('${inputString}'.toUpperCase()))`;
                    }
                    break;
                case Utils.SEARCH_ENUM.ncontains:
                    if (searchItem.isArray) {
                        outputString += '([' + filteredArr.map((i) => {
                            return "\'" + i.toString() + "\'";
                        }) + ']' + '.some(function(arrayItem){return !x.includes(arrayItem);},{x:x}))';
                    } else {
                        outputString += `(!x.includes('${inputString}'))`;
                    }
                    break;
                case Utils.SEARCH_ENUM.ncontainsCIS:
                    if (searchItem.isArray) {
                        outputString += '([' + filteredArr.map((i) => {
                            return "\'" + i.toString() + "\'";
                        }) + ']' + '.some(function(arrayItem){return !x.toUpperCase().includes(arrayItem.toUpperCase());},{x:x}))';
                    } else {
                        outputString += `(!x.toUpperCase().includes('${inputString}'.toUpperCase()))`;
                    }
                    break;
                case Utils.SEARCH_ENUM.startsWith:
                    if (searchItem.isArray) {
                        outputString += '([' + filteredArr.map((i) => {
                            return "\'" + i.toString() + "\'";
                        }) + ']' + '.some(function(arrayItem){return x.startsWith(arrayItem);},{x:x}))';
                    } else {
                        outputString += `(x.startsWith('${inputString}'))`;
                    }
                    break;
                case Utils.SEARCH_ENUM.endsWith:
                    if (searchItem.isArray) {
                        outputString += '([' + filteredArr.map((i) => {
                            return "\'" + i.toString() + "\'";
                        }) + ']' + '.some(function(arrayItem){return x.endsWith(arrayItem);},{x:x}))';
                    } else {
                        outputString += `(x.endsWith('${inputString}'))`;
                    }
                    break;
                case Utils.SEARCH_ENUM.equals:
                    if (searchItem.isArray) {
                        outputString += '([' + filteredArr.map((i) => {
                            return "\'" + i.toString() + "\'";
                        }) + ']' + '.some(function(arrayItem){return x == arrayItem;},{x:x}))';
                    } else {
                        if (columnType.isString()) {
                            outputString += `(x === '${inputString}')`;
                        } else {
                            outputString += `(x === ${inputString})`;
                        }
                    }
                    break;
                case Utils.SEARCH_ENUM.nequals:
                    if (searchItem.isArray) {
                        outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.map(function(arrayItem){' +
                            'if(x == arrayItem){return false;}else{return true;}},{x:x}))';
                    } else {
                        if (columnType.isString()) {
                            outputString += `(x !== '${inputString}')`;
                        } else {
                            outputString += `(x !== ${inputString})`;
                        }
                    }
                    break;
                case Utils.SEARCH_ENUM.greaterThan:
                    if (searchItem.isArray) {
                        if (columnType.isString()) {
                            outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.some(function(arrayItem){return x.localeCompare(arrayItem) > 0},{x:x}))';
                        } else {
                            outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.some(function(arrayItem){return x > arrayItem;},{x:x}))';
                        }
                    } else {
                        if (columnType.isString()) {
                            outputString += `(x.localeCompare('${inputString}') > 0)`;
                        } else {
                            outputString += `(x > ${inputString})`;
                        }
                    }
                    break;
                case Utils.SEARCH_ENUM.greaterThanOrEqual:
                    if (searchItem.isArray) {
                        if (columnType.isString()) {
                            outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.some(function(arrayItem){return x.localeCompare(arrayItem) >= 0},{x:x}))';
                        } else {
                            outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.some(function(arrayItem){return x >= arrayItem;},{x:x}))';
                        }
                    } else {
                        if (columnType.isString()) {
                            outputString += `(x.localeCompare('${inputString}') >= 0)`;
                        } else {
                            outputString += `(x >= ${inputString})`;
                        }
                    }
                    break;
                case Utils.SEARCH_ENUM.smallerThan:
                    if (searchItem.isArray) {
                        if (columnType.isString()) {
                            outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.some(function(arrayItem){return x < arrayItem;},{x:x}))';
                        } else {
                            outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.some(function(arrayItem){return x.localeCompare(arrayItem) < 0},{x:x}))';
                        }
                    } else {
                        if (columnType.isString()) {
                            outputString += `(x.localeCompare('${inputString}') < 0)`;
                        } else {
                            outputString += `(x < ${inputString})`;
                        }
                    }
                    break;
                case Utils.SEARCH_ENUM.smallerThanOrEqual:
                    if (searchItem.isArray) {
                        if (columnType.isString()) {
                            outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.some(function(arrayItem){return x.localeCompare(arrayItem) <= 0},{x:x}))';
                        } else {
                            outputString += '([' + filteredArr.map((i) => {
                                return "\'" + i.toString() + "\'";
                            }) + ']' + '.some(function(arrayItem){return x <= arrayItem;},{x:x}))';
                        }
                    } else {
                        if (columnType.isString()) {
                            outputString += `(x.localeCompare('${inputString}') <= 0)`;
                        } else {
                            outputString += `(x <= ${inputString})`;
                        }
                    }
            }
        }
        return outputString;
    };

    let searchEnabled = (searchItem) => {
        return searchItem.columnInput !== "" && searchItem.columnInput !== undefined;
    };

    let close = (reset) => {
        if (reset) {
            fireFilterEventCallBack(
                currentGridColumnFilterString,
                columnName
            );
        }
        visible = false;
    };

    let clearAndClose = () => {
        resetSearchFields();
        fireFilterEventCallBack(
            "",
            columnName
        );
        close(false);
    };

    let search = () => {
        let finalSearchString = constructFilterString();
        fireFilterEventCallBack(
            finalSearchString,
            columnName
        );
        close(false);
    }

    let csvDataIsValid = (data, columnType) => {
        let arr = [];
        arr = data.split(',');
        arr.forEach((item) => {
            if (isNaN(item) && columnType.isNumber()) {
                throw 'CSV does not only contain numbers.';
            }
        });
    };

    let components = () => {
        let result = [];
        searchFields.map((searchItem, index) => {
            result.push(
                m('div.dropdown-item', [
                    m("div.input-group", [
                        m("div.input-group-prepend", {
                            key: "input-group-prepend_" + gridId + "_" + index,
                        }, [
                            index !== 0 ?
                                m("div.dropdown-filter-switch", [
                                    m(Switch, {
                                        id: `andOrSwitch_${gridId}_${index}`,
                                        checked: searchItem.isAnd,
                                        "data-tippy-content": "AND / OR",
                                        "data-tippy-placement": "top",
                                        toggle: (state, ignore) => {
                                            searchItem.isAnd = state;
                                        }
                                    }),
                                    m(Label, {
                                        for: `andOrSwitch_${gridId}_${index}`,
                                        text: searchItem.isAnd ? "And" : "Or"
                                    })
                                ])
                                : "",
                            m(ImportButton, {
                                text: "",
                                id: `importFilter_${gridId}_${index}`,
                                enabled: true,
                                class: "btn-primary btn-sm",
                                icon: "fas fa-file-import",
                                "data-tippy-content": "Import a csv file with values",
                                "data-tippy-placement": "top",
                                onchange: (e) => {
                                    if (isImportingFilter) {
                                        e.preventDefault();
                                        return;
                                    }
                                    isImportingFilter = true;

                                    readCsvFromDisk(e).then((data) => {
                                        try {
                                            searchItem.isArray = true;
                                            isImportingFilter = false;
                                            m.redraw();
                                            csvDataIsValid(data, columnType);
                                            setTimeout(() => {
                                                searchItem.columnInput = data;
                                                m.redraw();
                                            }, 0);
                                        } catch (e) {
                                            CmMithrilGlobal.toasts.push({
                                                type: "failure",
                                                message: e,
                                            });
                                            searchItem.isArray = false;
                                            isImportingFilter = false;
                                            clearAndClose();
                                            m.redraw();
                                        }
                                    });
                                }
                            })
                        ]),
                        m(Input, {
                            id: "searchInput_" + gridId + "_" + index,
                            key: "searchInput_" + gridId + "_" + index + "_" + searchItem.isArray,
                            placeHolder: 'Filter value',
                            enabled: !searchItem.isArray,
                            value: searchItem.columnInput,
                            type: !searchItem.isArray && columnType.isNumber() ? 'number' : 'text',
                            pattern: !searchItem.isArray && columnType.isNumber() ? "^[0-9]" : "",
                            attrs: !searchItem.isArray && columnType.isNumber() ? {min: 0, step: 1} : {},
                            oninput: (e) => {
                                searchItem.columnInput = e.target.value;
                            },
                            onkeyup: (e) => {
                                if (e.keyCode === 13) {
                                    search();
                                }
                            }
                        }),
                        m("div.input-group-append", {
                            key: "input-group-append_" + gridId + "_" + index + "_" + columnType.name,
                        }, [
                            m(Button, {
                                id: "filterButton_" + gridId + "_" + index,
                                class: "btn-sm btn-primary dropdown-toggle-button",
                                text: "",
                                icon: searchItem.searchEnum.icon,
                                iconOnTheLeft: false,
                                'data-tippy-content': "Search '" + searchItem.searchEnum.description + "'",
                                'data-tippy-placement': "top",
                                enabled: searchEnabled(searchItem),
                                onclick: (e) => {
                                    e.preventDefault();
                                    e.stopPropagation();
                                },
                            }),
                            m(Button, {
                                id: "filterDropDownButton_" + gridId + "_" + index,
                                class: "btn-sm btn-success dropdown-toggle dropdown-toggle-split",
                                enabled: searchEnabled(searchItem),
                                attrs: {"data-toggle": "dropdown", "data-boundary": "window"}
                            }),
                            m("div.dropdown-menu.dropdown-menu-right", [
                                    _.keys(Utils.SEARCH_ENUM).map((key) => {
                                        let tmpSearchEnum = Utils.SEARCH_ENUM[key];
                                        return m(Button, {
                                            class: "dropdown-item",
                                            text: tmpSearchEnum.description,
                                            icon: tmpSearchEnum.icon,
                                            onclick: () => {
                                                searchItem.searchEnum = tmpSearchEnum;
                                            }
                                        });
                                    })
                                ]
                            )
                        ]),
                        index !== 0 ?
                            m("div", {
                                key: "delete_" + gridId + "_" + index,
                            }, [
                                m(Button, {
                                    id: "deleteSearchItemFilterButton_" + gridId + "_" + index,
                                    class: "btn-sm btn-danger ml-1",
                                    text: "",
                                    icon: "fa-times",
                                    iconOnTheLeft: false,
                                    'data-tippy-content': "Remove from filter",
                                    'data-tippy-placement': "top",
                                    onclick: () => {
                                        searchFields.splice(index, 1);
                                    }
                                })
                            ]) : m("div", {key: "delete_" + gridId + "_" + index})
                    ])
                ])
            )
        })
        result.push(
            m('div.dropdown-item.dropdown-filter-buttons', [
                m(Button, {
                    class: "btn-info btn-sm",
                    text: "Add",
                    icon: "fa-plus",
                    onclick: () => {
                        searchFields.push({
                            searchEnum: columnType.defaultSearchEnum,
                            importSearchEnum: columnType.defaultSearchEnum,
                            columnInput: undefined,
                            columnFilterString: undefined,
                            isArray: false
                        });
                    }
                }),
                m(Button, {
                    class: "btn-danger btn-sm",
                    text: "Clear",
                    icon: "fa-undo",
                    onclick: () => {
                        clearAndClose();
                    }
                }),
                m(Button, {
                    class: "btn-success btn-sm",
                    text: "Search",
                    icon: "fa-search",
                    onclick: () => {
                        search();
                    }
                })
            ])
        );
        return result;
    }

    return {
        oninit: ({attrs: {id}}) => {
            if (id === undefined) {
                throw new Error("SlickDropdownFilter must have an 'id' attribute.");
            }
            gridId = id;
        },
        oncreate: () => {
            $(document).on('click.' + "SlickDropdownFilter_" + gridId, (event) => {
                if (visible && $(event.target).closest("#SlickDropdownFilter_" + gridId).length === 0) {
                    close(true);
                    m.redraw();
                }
            });
            $(document).on('keyup.' + "SlickDropdownFilter_" + gridId, (e) => {
                if (visible && e.keyCode === 27) {
                    close(true);
                    m.redraw();
                }
            });
            initSearchFields();
        },
        onremove: () => {
            $(document).off("click.SlickDropdownFilter_" + gridId);
            $(document).off("keyup.SlickDropdownFilter_" + gridId);
        },
        view: ({attrs: {colName, selectedCmColumn, colFilterString, popped, fireFilterEvent}}) => {
            fireFilterEventCallBack = fireFilterEvent;
            visible = popped;
            cmColumn = selectedCmColumn;
            currentGridColumnFilterString = colFilterString;
            if (columnName !== colName) {
                columnType = SlickGridConstants.fromName(cmColumn.type);
                columnName = colName;
                if (cmColumn["dropDownFilter"] !== undefined) {
                    searchFields = cmColumn["dropDownFilter"];
                } else {
                    initSearchFields();
                    cmColumn["dropDownFilter"] = searchFields;
                }
            }
            // return components();
            return m(`div#SlickDropdownFilter_${gridId}`, {
                "data-show": visible,
                class: `slick-dropdown-filter bg-light border`
            }, [
                m("div#arrow", {"data-popper-arrow": ''}),
                m('div.slick-dropdown-filter-container', components())
            ])
        }
    }
}

export default SlickDropdownFilter;