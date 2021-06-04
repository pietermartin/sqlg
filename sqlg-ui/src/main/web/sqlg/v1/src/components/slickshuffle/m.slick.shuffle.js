import m from "mithril";
import SlickGrid2 from "../slickgrid/m.slick.grid";
import Button from "../form/button";

function SlickShuffle() {
    let checkedActiveItems = [];
    let checkedInactiveItems = [];
    let activeData = {};
    let inactiveData = {};
    let refreshShuffle = true;
    let shuffleClick = false;
    let refreshShuffleData = false;
    let activeColumnFilterCache = "";
    let inactiveColumnFilterCache = "";

    let gridData = {
        constructor: function (colName) {
            return {
                columns: [{
                    cm_column: {type: "STRING", isEditable: false, isRequired: false},
                    field: "name",
                    id: "name",
                    name: colName
                }],
                data: []
            }
        }
    };

    let dynamicSort = function (property) {
        var sortOrder = 1;
        return function (a, b) {
            var result = (a[property].toLowerCase() < b[property].toLowerCase()) ? -1 : (a[property].toLowerCase() > b[property].toLowerCase()) ? 1 : 0;
            return result * sortOrder;
        }
    }

    let shuffle = function (item, direction) {
        if (direction) {
            activeData.data.push(item);
            inactiveData.data.splice(inactiveData.data.findIndex(i => i.id === item.id), 1);
        } else if (!direction) {
            inactiveData.data.push(item);
            activeData.data.splice(activeData.data.findIndex(i => i.id === item.id), 1);
            shuffleClick = true;
        }
        activeData.data.sort(dynamicSort("name"));
        inactiveData.data.sort(dynamicSort("name"));
    };

    let multiShuffle = function (items, direction) {
        items.forEach(function (i) {
            delete i.selected;
            shuffle(i, direction);
        });
    };

    let slickShuffleActiveFilterInputCallBack = function (filterColumn) {
        activeColumnFilterCache = filterColumn;
    };

    let slickShuffleInactiveFilterInputCallBack = function (filterColumn) {
        inactiveColumnFilterCache = filterColumn;
    };

    return {
        oninit: function (vnode) {
            if (vnode.attrs.notSelectedHeading !== undefined && vnode.attrs.notSelectedHeading !== null) {
                inactiveData = new gridData.constructor(vnode.attrs.notSelectedHeading);
            } else {
                inactiveData = new gridData.constructor("Not Selected");
            }
            if (vnode.attrs.selectedHeading !== undefined && vnode.attrs.selectedHeading !== null) {
                activeData = new gridData.constructor(vnode.attrs.selectedHeading);
            } else {
                activeData = new gridData.constructor("Selected");
            }
        },
        onupdate: function (vnode) {
            if (shuffleClick) {
                refreshShuffle = true;
                shuffleClick = false;
            } else if (!shuffleClick) {
                refreshShuffle = false;
            }
        },
        view: function (vnode) {
            let activeColumnFilters = (vnode.attrs.activeColumnFilters !== undefined ? vnode.attrs.activeColumnFilters : activeColumnFilterCache);
            let inactiveColumnFilters = (vnode.attrs.inactiveColumnFilters !== undefined ? vnode.attrs.inactiveColumnFilters : inactiveColumnFilterCache);

            if (!refreshShuffleData || vnode.attrs.refresh) {

                if ((typeof vnode.attrs.data !== "undefined")) {
                    if ((typeof vnode.attrs.selectedItems !== "undefined")) {
                        if (vnode.attrs.selectedItems.length > 0 && vnode.attrs.data.length > 0) {

                            activeData.data = vnode.attrs.selectedItems;
                            inactiveData.data = vnode.attrs.data;

                            inactiveData.data = inactiveData.data.filter((inactiveItem) =>
                                activeData.data.every((activeItem) => activeItem.id !== inactiveItem.id));

                            refreshShuffleData = true;
                            refreshShuffle = true;
                        } else if (vnode.attrs.selectedItems.length === 0 && vnode.attrs.data.length > 0) {
                            activeData.data = vnode.attrs.selectedItems;
                            inactiveData.data = vnode.attrs.data;
                            refreshShuffleData = true;
                            refreshShuffle = true;
                        }
                    } else {
                        activeData.data = vnode.attrs.selectedItems;
                        inactiveData.data = vnode.attrs.data;
                        refreshShuffleData = true;
                        refreshShuffle = true;
                    }
                }
            }
            // debugger;
            return m("div", {id: vnode.attrs.id, class: "slickShuffle"}, [
                m(SlickGrid2, {
                    id: "slickShuffleActive" + "_" + vnode.attrs.id,
                    data: activeData,
                    columnFilters: activeColumnFilters,
                    multiSelect: true,
                    rebuildGrid: refreshShuffle,
                    showSpinner: false,
                    checkedItems: checkedActiveItems,
                    options: {
                        multiSelectCheckBox: true,
                    },
                    hideGroupHeader: true,
                    hideFooter: true,
                    dblClickCallBack: function (item) {
                        refreshShuffle = true;
                        shuffle(item, false);
                        if (vnode.attrs.getChanges !== undefined) {
                            vnode.attrs.getChanges(activeData);
                        }
                        m.redraw();
                    },
                    checkedItemsCallBack: function (item) {
                        m.redraw();
                    },
                    filterInputCallBack: (vnode.attrs.activeFilterInputCallBack !== undefined ? vnode.attrs.activeFilterInputCallBack : slickShuffleActiveFilterInputCallBack)
                }),
                m("div", {class: "shuffleButtons"}, [
                    m("div"),
                    m(Button, {
                        text: "",
                        class: "btn-primary btn-sm",
                        icon: "fa-arrow-alt-circle-right",
                        enabled: checkedActiveItems.length > 0 && checkedInactiveItems.length === 0,
                        onclick: function (e) {
                            refreshShuffle = true;
                            multiShuffle(checkedActiveItems, false);
                            checkedActiveItems = [];
                            if (vnode.attrs.getChanges !== undefined) {
                                vnode.attrs.getChanges(activeData);
                            }
                            m.redraw();
                        }
                    }),
                    m(Button, {
                        text: "",
                        class: "btn-primary btn-sm",
                        icon: "fa-arrow-alt-circle-left",
                        enabled: checkedInactiveItems.length > 0 && checkedActiveItems.length === 0,
                        onclick: function (e) {
                            refreshShuffle = true;
                            multiShuffle(checkedInactiveItems, true);
                            checkedInactiveItems = [];
                            if (vnode.attrs.getChanges !== undefined) {
                                vnode.attrs.getChanges(activeData);
                            }
                            m.redraw();
                        }
                    }),
                    m("div")
                ]),
                m(SlickGrid2, {
                    id: "slickShuffleInactive" + "_" + vnode.attrs.id,
                    data: inactiveData,
                    columnFilters: inactiveColumnFilters,
                    multiSelect: true,
                    rebuildGrid: refreshShuffle,
                    showSpinner: false,
                    checkedItems: checkedInactiveItems,
                    options: {
                        multiSelectCheckBox: true,
                    },
                    hideGroupHeader: true,
                    hideFooter: true,
                    dblClickCallBack: function (item) {
                        refreshShuffle = true;
                        shuffle(item, true);
                        if (vnode.attrs.getChanges !== undefined) {
                            vnode.attrs.getChanges(activeData);
                        }
                        m.redraw();
                    },
                    checkedItemsCallBack: function (item) {
                        m.redraw();
                    },
                    filterInputCallBack: (vnode.attrs.inactiveFilterInputCallBack !== undefined ? vnode.attrs.activeFilterInputCallBack : slickShuffleInactiveFilterInputCallBack)
                }),
            ]);
        }
    }
}

export default SlickShuffle;