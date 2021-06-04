// Michael Van Der Merwe 15:00pm - 22/02/2019
import $ from "jquery";
import _ from "underscore";

function SlickGridGroupPopUp(dataView, grid) {
    var _self;
    var slickGroup = {"id": 0, "reset": [], "set": [], "count": 0, "collapse": 0, "sum": ""},
        clickX = 0,
        clickY = 0;


    function showGroupPopup(grid, e) {
        clickX = e.pageX;
        clickY = e.pageY;
        var i, c, s = "<option value=''>Sum Column";

        if (!slickGroup["start"]) {
            c = grid.getColumns();
            for (i in c) {
                if (c[i].name && c[i].name.indexOf("<") < 0) {
                    slickGroup["reset"].push({"field": c[i].field, "name": c[i].name});
                    slickGroup["set"].push({"field": c[i].field, "name": c[i].name, "on": 0});
                }
                slickGroup["start"] = 1;
            }
        }

        if ($("#slickGroupPopUp").length)
            slickGroupClose();
        else {
            var group_popup = $("<div id=slickGroupPopUp></div>");
            group_popup.addClass("slickGroup");
            group_popup.addClass("slickPopUps");
            var head = $("<b><i class='fas fa-link'></i> Grouping Setup</b>");
            head.addClass("slickGroupHead");
            head.addClass("slickPopUps");
            var closer = $("<i></i>").on('click', function () {
                slickGroupClose();
            });
            closer.addClass("fas");
            closer.addClass("fa-times");
            closer.addClass("slickGroupClose");
            closer.addClass("slickPopUps");
            head.append(closer);
            group_popup.append(head);
            var body = $("<div></div>");
            body.addClass("slickGroupBody");
            body.addClass("slickPopUps");
            var em;

            _.each(slickGroup["set"], function (item) {
                if (item.field != "id" && item.field != "delete") {
                    em = $("<em data-field=\"" + item.field + "\" data-name=\"" + item.name + "\" data-on=" + item.on + " class=slickPopUps></em>");
                    em.append($("<input type=checkbox " + (item.on ? " checked" : "") + " class=slickPopUps />").on('click', function (event) {
                        slickGroupCheckbox(event, grid)
                    }));
                    em.append($("<span class=slickPopUps>" + item.name + "</span>"));
                    em.append($("<i class='fas fa-arrow-circle-up slickGroupUp slickPopUps'></i>").on("click", function (event) {
                        slickGroupCheckbox(event, grid, 1);
                    }));
                    body.append(em);

                    s += "<option value=\"" + item.field + "\"" + (slickGroup["sum"] == item.field ? " selected" : "") + ">Sum: " + item.name;
                }
            });
            group_popup.append(body);

            var foot = $("<div" + (slickGroup["count"] ? "" : " style=display:none") + "></div>");
            foot.addClass("slickGroupFoot");
            foot.addClass("slickPopUps");
            em = $("<em class=slickPopUps></em>");
            em.append($("<input type=checkbox class=slickPopUps" + (slickGroup["collapse"] ? " checked" : "") + " />")
                .on("click", function (event) {
                    slickGroupCollapse(event, grid)
                }));
            em.append($("<span class=slickPopUps>Collapse grid</span>"));
            foot.append(em);
            em = $("<em class=slickPopUps></em>");
            em.append($("<input type=checkbox class=slickPopUps />")
                .on("click", function (event) {
                    slickGroupCheckbox(event, grid, 0, 1)
                }));
            em.append($("<span class=slickPopUps>Clear groups</span>"));
            foot.append(em);
            em = $("<em class=slickPopUps></em>");
            em.append($("<select id=sumCol class=slickPopUps style='width:100%;margin:3px 0;padding:3px 0;background:#EEE;border-width:1px 0 0 0'>" + s + "</select>")
                .on("change", function (event) {
                    slickGroupCheckbox(event, grid, 0, 0, 1)
                }));
            foot.append(em);
            group_popup.append(foot);
            $('body').append(group_popup);

            group_popup = $("#slickGroupPopUp");
            group_popup.draggable();
            group_popup.css({position: "fixed"});
            em = clickX - group_popup.width();
            if (em < 0)
                em = 0;
            group_popup.css({left: em + "px"});
            group_popup.css({top: clickY + "px"});
            group_popup.css({opacity: 1});
            setTimeout(function () {
                $('#slickGroupPopUp').css({transition: 'box-shadow 1s, opacity 1s'});
            }, 1000);
        }
    }


    function slickGroupCheckbox(checkbox, grid, moveUp, turnOff, sums) {
        var groupings = [], o, p, i;
        slickGroup["set"] = [];
        checkbox = checkbox.target;

        if (turnOff) {
            checkbox.checked = false;
            checkbox.parentNode.parentNode.style.display = "none";
            slickGroup["count"] = 0;
            o = checkbox.parentNode.parentNode.previousSibling.childNodes;
            i = 0;
            while (o[i]) {
                o[i].dataset.on = 0;
                o[i].firstChild.checked = false;
                slickGroup["set"].push({"field": o[i].dataset.field, "name": o[i].dataset.name, "on": 0});
                i++;
            }
            slickGroup["sum"] = "";
        } else {
            if (sums) {
                if (slickGroup["sum"]) {
                    slickGroup["sum"] = "";
                    checkbox.checked = false;
                } else
                    slickGroup["sum"] = checkbox.value;
                p = checkbox.parentNode.parentNode.previousSibling;
            } else {
                o = checkbox.parentNode;
                p = o.parentNode;

                if (moveUp) {
                    if (o.previousSibling)
                        o.parentNode.insertBefore(o, o.previousSibling);
                    else
                        while (o.nextSibling && o.nextSibling.dataset.on == 1)
                            o.parentNode.insertBefore(o.nextSibling, o);
                } else {
                    o.dataset.on = checkbox.checked == true ? 1 : 0;

                    if (checkbox.checked == true)
                        while (o.previousSibling && o.previousSibling.dataset.on != 1)
                            o.parentNode.insertBefore(o, o.previousSibling);
                    else
                        while (o.nextSibling && o.nextSibling.dataset.on == 1)
                            o.parentNode.insertBefore(o.nextSibling, o);
                }
            }

            _.each(p.childNodes, function (item) {
                if (item.dataset.on == 1) {
                    if (slickGroup["sum"])
                        groupings.push({
                            getter: item.dataset.field, formatter: function (g) {
                                return "<b>" + item.dataset.name + "</b>: " + g.value + "  <i>" + g.count + " item" + (g.count != 1 ? "s" : "") + "</i>";
                            },
                            aggregators: [new Slick.Data.Aggregators.Sum(slickGroup["sum"])],
                            aggregateCollapsed: false,
                            lazyTotalsCalculation: true
                        });
                    else
                        groupings.push({
                            getter: item.dataset.field, formatter: function (g) {
                                return "<b>" + item.dataset.name + "</b>: " + g.value + "  <i>" + g.count + " item" + (g.count != 1 ? "s" : "") + "</i>";
                            }
                        });
                }

                slickGroup["set"].push({
                    "field": item.dataset.field,
                    "name": item.dataset.name,
                    "on": parseInt(item.dataset.on)
                });
            });

            if (slickGroup["sum"]) {
                for (i in grid.getColumns()) {
                    if (slickGroup["sum"] == grid.getColumns()[i].field) {
                        grid.getColumns()[i].groupTotalsFormatter = sumFormatter;
                        break;
                    }
                }
            }

            slickGroup["count"] = groupings.length;
            p.nextSibling.style.display = slickGroup["count"] ? "" : "none";

            o = $('#slickGroupPopUp');
            o.css({transition: "all 0.5s", top: (clickY) + "px"});

            setTimeout(function () {
                $('#slickGroupPopUp').css({transition: 'all 0s'})
            }, 700);
        }

        grid.getData().setGrouping(groupings);

        if (slickGroup["count"] && slickGroup["collapse"])
            grid.getData().collapseAllGroups();

        p = document.getElementById("sumCol");
        if (!slickGroup["sum"])
            p.value = "";
        p.blur();

        grid.group = slickGroup["count"] ? slickGroup : 0; // keep settings here for refresh-reApply
        grid.invalidate();
        grid.render();
        grid.invalidateRows([]);
        var rng = grid.getRenderedRange();
        grid.getData().setRefreshHints({
            ignoreDiffsBefore: rng.top,
            ignoreDiffsAfter: rng.bottom
        });
    }

    function collapseGroup() {
        grid.getData().collapseAllGroups();
        grid.invalidate();
        slickGroup["collapse"] = 1;
    }

    function slickGroupCollapse(checkbox, grid) {
        checkbox = checkbox.target.checked;
        if (checkbox == true)
            grid.getData().collapseAllGroups();
        else
            grid.getData().expandAllGroups();
        grid.invalidate();
        slickGroup["collapse"] = checkbox == true ? 1 : 0;
    }


    function sumFormatter(totals, columnDef) {
        var v = totals.sum && totals.sum[columnDef.field];
        if (v != null)
            return "<b style=color:" + (isNaN(v) ? "red> Error" : "blue>" + v) + "</b>";
        return "";
    }


    function slickGroupClose() {
        var popUp = $("#slickGroupPopUp");
        if (popUp.length) {
            popUp.css({opacity: 0, transition: 'all 0.5s', left: 0});
            setTimeout(function () {
                $('#slickGroupPopUp').remove();
            }, 1000);
        }
    }


    grid.slickGroupApply = function (grouping, show) {
        var groupings = [], i;

        if (grouping)
            slickGroup = grouping;

        for (i in slickGroup.set) {
            if (slickGroup.set[i].on == 1) {
                if (slickGroup["sum"])
                    groupings.push({
                        getter: slickGroup.set[i].field,
                        formatter: (function (i) {
                            return function (g) {
                                return "<b>" + slickGroup.set[i].name + "</b>: " + g.value + "  <i>" + g.count + " item" + (g.count != 1 ? "s" : "") + "</i>";
                            }
                        })(i),
                        aggregators: [new Slick.Data.Aggregators.Sum(slickGroup["sum"])],
                        aggregateCollapsed: false,
                        lazyTotalsCalculation: true
                    });
                else
                    groupings.push({
                        getter: slickGroup.set[i].field,
                        formatter: (function (i) {
                            return function (g) {
                                return "<b>" + slickGroup.set[i].name + "</b>: " + g.value + "  <i>" + g.count + " item" + (g.count != 1 ? "s" : "") + "</i>";
                            }
                        })(i)
                    });
            }
        }

        if (slickGroup["sum"]) {
            for (i in grid.getColumns()) {
                if (slickGroup["sum"] == grid.getColumns()[i].field) {
                    grid.getColumns()[i].groupTotalsFormatter = sumFormatter;
                    break;
                }
            }
        }

        grid.getData().setGrouping(groupings);

        if (slickGroup["count"] && slickGroup["collapse"])
            grid.getData().collapseAllGroups();

        grid.invalidate();
        grid.render();
        grid.invalidateRows([]);
        var rng = grid.getRenderedRange();
        grid.getData().setRefreshHints({
            ignoreDiffsBefore: rng.top,
            ignoreDiffsAfter: rng.bottom
        });

        dataView.refresh();

        if (show)
            showGroupPopup(grid);
    };
    // end
    $.extend(this, {
        "collapseGroup": collapseGroup,
        "showGroupPopup": showGroupPopup,
        "slickGroupApply": grid.slickGroupApply
    });

    _self = this;
}

// Slick.grid
$.extend(true, window, {Slick: {Controls: {SlickGridGroupPopUp: SlickGridGroupPopUp}}});