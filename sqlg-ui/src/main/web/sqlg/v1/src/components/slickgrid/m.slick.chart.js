import m from 'mithril';
import Label from "../form/label";
import SlickSelect from "../slickselect/m.slick.select";
import "./m.slick.chart.scss";
import _ from "underscore";

import Highcharts from "highcharts/highstock";
import avocadoTheme from "../../assets/highcharts/themes/avocado"
import darkBlueTheme from "../../assets/highcharts/themes/dark-blue";
import darkGreedTheme from "../../assets/highcharts/themes/dark-green";
import darkUnicaTheme from "../../assets/highcharts/themes/dark-unica"
import grayTheme from "../../assets/highcharts/themes/gray"
import gridTheme from "../../assets/highcharts/themes/grid"
import gridLightTheme from "../../assets/highcharts/themes/grid-light"
import highContractDarkTheme from "../../assets/highcharts/themes/high-contrast-dark"
import highContractLightTheme from "../../assets/highcharts/themes/high-contrast-light"
import sandSignikaTheme from "../../assets/highcharts/themes/sand-signika"
import skiesTheme from "../../assets/highcharts/themes/skies"
import sunsetTheme from "../../assets/highcharts/themes/sunset"
import Button from "../form/button";
import Switch from "../switch/switch";

function SlickChart(ignore) {

    let workspace;
    let searchObject;
    let chart;
    let chartId;
    let drillInLevel = 0;
    let groupParameters = false;

    const reductions = [
        {id: "total_allocation", name: "Distribution"},
        {id: "least_used_value", name: "Least used value"},
        {id: "most_used_value", name: "Most used value"},
        {id: "sum", name: "Sum"},
        {id: "min", name: "Minimum"},
        {id: "max", name: "Maximum"},
        {id: "avg", name: "Average"},
        {id: "median", name: "Median"}
    ];
    let selectedReductions = [{id: "total_allocation", name: "Distribution"}];
    let refreshReductions = false;

    const chartTypes = [
        {id: "spline", name: "spline"},
        {id: "line", name: "line"},
        {id: "areaspline", name: "areaspline"},
        {id: "area", name: "area"},
        {id: "column", name: "column"},
        {id: "column3d", name: "column 3D"},
        {id: "pie", name: "pie"},
        {id: "pie3d", name: "pie 3D"},
        {id: "donut", name: "donut"},
        {id: "donut3d", name: "donut 3D"}
        // {id: "packedbubble", name: "packedbubble"}
    ];
    let selectedChartTypes = [{id: "column", name: "column"}];
    let refreshChartTypes = false;

    const chartThemes = [
        {id: "avocado", name: "avocado"},
        {id: "dark-blue", name: "dark-blue"},
        {id: "dark-green", name: "dark-green"},
        {id: "dark-unica", name: "dark-unica"},
        {id: "gray", name: "gray"},
        {id: "grid", name: "grid"},
        {id: "grid-light", name: "grid-light"},
        {id: "high-contrast-dark", name: "high-contrast-dark"},
        {id: "high-contrast-light", name: "high-contrast-light"},
        {id: "sand-signika", name: "sand-signika"},
        {id: "skies", name: "skies"},
        {id: "sunset", name: "sunset"}
    ];
    let selectedChartThemes = [{id: "grid-light", name: "grid-light"}];
    let refreshChartThemes = false;

    let selectedColumns = [];
    let refreshColumns = false;
    let columns = [];

    let selectedGroupOverColumns = [];
    let refreshGroupOverColumns = false;
    let groupOverColumns = [];

    let title;
    let previousTitles = [];
    let chartOptions = {
        chart: {
            type: 'column',
            zoomType: 'x',
            events: {
                drilldown: function (e) {
                    drillInLevel++;
                    previousTitles.push(title);
                    title = chartTitle(e)
                    chart.setTitle({text: title});
                },
                drillup: function (e) {
                    drillInLevel--;
                    title = previousTitles.pop();
                    chart.setTitle({text: title});
                }
            }
        },
        tooltip: {
            headerFormat: '<span style="font-size:10px">{series.name}</span><table>',
            // pointFormat: '<tr><td style="color:{series.color};padding:0">{point.y}: </td><td style="padding:0"><b>{point.name}</b></td></tr>',
            pointFormat: '<tr><td padding:0">{point.y}: </td><td style="padding:0"><b>{point.name}</b></td></tr>',
            footerFormat: '</table>',
            // shared: true,
            useHTML: true
        },
        plotOptions: {
            series: {
                //this is needed to not loose the series labels when navigating back from a drill-in
                cropThreshold: 300
            },
            packedbubble: {
                minPointSize: 15,
                maxPointSize: 200,
                // minSize: 15,
                // maxSize: 300,
                zMin: 0,
                zMax: 1000,
                layoutAlgorithm: {
                    gravitationalConstant: 0.01,
                    friction: -0.981,
                    splitSeries: false,
                    // splitSeries: true,
                    seriesInteraction: false,
                    dragBetweenSeries: false,
                    parentNodeLimit: true
                },
                dataLabels: {
                    enabled: true,
                    format: '{point.name}',
                    // filter: {
                    //     property: 'y',
                    //     operator: '>',
                    //     value: 250
                    // },
                    style: {
                        color: 'black',
                        textOutline: 'none',
                        fontWeight: 'normal'
                    }
                }
            }
        }
    };

    let parseValues = function (value) {
        //remove spaces
        if (value != null && !isNaN(value)) {
            value = value.replace(/\s+/g, '');
            if (value === '<NULL>') {
                value = 0;
            }
            value = parseFloat(value);
            if (!isNaN(value)) {
                return value;
            } else {
                return 0;
            }
        } else {
            return 0;
        }
    }

    let chartTitle = function (e) {
        if (drillInLevel !== 0 && drillInLevel !== 1 && drillInLevel !== 2) {
            throw Error("Unhandled drill in level: " + drillInLevel);
        }
        if (drillInLevel === 0) {
            if (selectedGroupOverColumns.length > 0) {
                let part = selectedReductions[0].name + " of " + _.reduce(selectedColumns, function (a, b) {
                    if (a === "") {
                        return b.name;
                    } else {
                        return a + ', ' + b.name
                    }
                }, "");
                return part + " per " + selectedGroupOverColumns[0].name;
            } else {
                return selectedReductions[0].name + " of " + _.reduce(selectedColumns, function (a, b) {
                    if (a === "") {
                        return b.name;
                    } else {
                        return a + ', ' + b.name
                    }
                }, "");
            }
        }
        switch (selectedReductions[0].id) {
            case "total_allocation":
                if (drillInLevel === 1) {
                    return `Distinct allocation of ${e.seriesOptions.name}`;
                }
            {
                return `Raw values for ${e.seriesOptions.name}`;
            }
            case "min":
                if (drillInLevel === 1) {
                    return `Min for ${e.seriesOptions.name}`;
                } else {
                    return `Raw values for ${e.seriesOptions.name}`;
                }
            case "max":
                if (drillInLevel === 1) {
                    return `Max for ${e.seriesOptions.name}`;
                } else {
                    return `Raw values for ${e.seriesOptions.name}`;
                }
            case "sum":
                if (drillInLevel === 1) {
                    return `Sum for ${e.seriesOptions.name}`;
                } else {
                    return `Raw values for ${e.seriesOptions.name}`;
                }
            case "avg":
                if (drillInLevel === 1) {
                    return `Avg for ${e.seriesOptions.name}`;
                } else {
                    return `Raw values for ${e.seriesOptions.name}`;
                }
            default:
                throw Error("Unknown reduction " + reduction.id);
        }
    }

    let getTheme = function (theme) {
        switch (theme) {
            case "avocado":
                return avocadoTheme;
            case "dark-blue":
                return darkBlueTheme;
            case "dark-green":
                return darkGreedTheme;
            case "dark-unica":
                return darkUnicaTheme;
            case "gray":
                return grayTheme;
            case "grid":
                return gridTheme;
            case "grid-light":
                return gridLightTheme;
            case "high-contrast-dark":
                return highContractDarkTheme;
            case "high-contrast-light":
                return highContractLightTheme;
            case "sand-signika":
                return sandSignikaTheme;
            case "skies":
                return skiesTheme;
            case "sunset":
                return sunsetTheme;
            default:
                throw Error("Unhandled highcharts theme " + selectedChartThemes[0].id);

        }
    }

    let isPieLike = function () {
        return (selectedChartTypes[0].id === 'pie' || selectedChartTypes[0].id === 'pie3d' || selectedChartTypes[0].id === 'donut' || selectedChartTypes[0].id === 'donut3d');
    }

    let showChart = function () {
        drillInLevel = 0;
        if (selectedColumns.length < 1) {
            throw Error("showChart can not be called without columns");
        }
        if (selectedReductions.length !== 1) {
            throw Error("showChart can not be called without a reduction");
        }
        title = chartTitle(null);
        chartOptions.chart.type = selectedChartTypes[0].id;
        if (chartOptions.chart.type === 'column3d') {
            chartOptions.chart.type = 'column';
            chartOptions.chart.options3d = {
                enabled: true,
                alpha: 25,
                beta: 15,
                depth: 50,
                viewDistance: 25
            }
        } else if (chartOptions.chart.type === 'column') {
            chartOptions.chart.type = 'column';
            chartOptions.plotOptions.column = {
                minPointLength: 3,
                pointPadding: 0.2,
                borderWidth: 0,
                depth: 25
            }
            delete chartOptions.chart.options3d;
        } else if (isPieLike()) {
            chartOptions.plotOptions.pie = {
                allowPointSelect: true,
                cursor: 'pointer',
                dataLabels: {
                    enabled: true,
                    format: '{point.name}'
                }
            }
            if (chartOptions.chart.type === 'pie3d') {
                chartOptions.chart.type = 'pie';
                chartOptions.plotOptions.pie = {
                    depth: 45,
                }
                chartOptions.chart.options3d = {
                    enabled: true,
                    alpha: 45,
                    beta: 15
                }
            } else if (chartOptions.chart.type === 'pie') {
                chartOptions.chart.type = 'pie';
                delete chartOptions.chart.options3d;
            } else if (chartOptions.chart.type === 'donut') {
                chartOptions.chart.type = 'pie';
                chartOptions.plotOptions.pie = {
                    innerSize: 200,
                }
                delete chartOptions.chart.options3d;
            } else if (chartOptions.chart.type === 'donut3d') {
                chartOptions.chart.type = 'pie';
                chartOptions.chart.options3d = {
                    enabled: true,
                    alpha: 45
                }
                chartOptions.plotOptions.pie = {
                    innerSize: 200,
                    depth: 45
                }
            }
        }
        chartOptions.title = {
            text: title
        };
        if (selectedChartTypes[0].id !== 'packedbubble') {
            chartOptions.xAxis = {
                type: 'category'
            }
            chartOptions.yAxis = {
                min: 0,
                title: {
                    text: "value"
                }
            };
        } else {
            delete chartOptions.xAxis;
            delete chartOptions.yAxis;
        }
        if (selectedGroupOverColumns.length === 0) {
            if (selectedChartTypes[0].id === 'packedbubble') {
                reducePackedBubble();
            } else {
                reduce();
            }
        } else {
            reduceGrouped();
        }
        if (chart !== undefined) {
            chart.destroy();
        }
        chart = Highcharts.chart(chartId, Highcharts.merge(chartOptions, getTheme(selectedChartThemes[0].id)));
    };

    let reduce = function () {
        // noinspection DuplicatedCode
        let reduction = selectedReductions[0];
        let reduced = {};
        chartOptions.series = [];
        chartOptions.drilldown = {series: []};

        let groupedParameterName;
        if (!groupParameters) {
            if (isPieLike()) {
                for (let i = 0; i < selectedColumns.length; i++) {
                    let col = selectedColumns[i];
                    reduced[col.id] = {};
                }
                chartOptions.series.push({name: "pieLike", data: []});
            } else {
                for (let i = 0; i < selectedColumns.length; i++) {
                    let col = selectedColumns[i];
                    reduced[col.id] = {};
                    chartOptions.series.push({name: col.name, data: []});
                }
            }
        } else {
            let names = _.map(selectedColumns, function (item) {
                return item.name;
            });
            groupedParameterName = _.reduce(names, function (a, b) {
                return a + "-" + b;
            });
            reduced[groupedParameterName] = {};
        }

        if (reduction.id === 'median') {
            if (searchObject.searchResults.data.length % 2 === 0) {
                for (let j = 0; j < selectedColumns.length; j++) {
                    let col = selectedColumns[j];
                    let sortedData = _.sortBy(searchObject.searchResults.data, function (item) {
                        return parseValues(item[col.id]);
                    })
                    let itemA = sortedData[(searchObject.searchResults.data.length / 2) - 1];
                    let itemB = sortedData[searchObject.searchResults.data.length / 2];
                    let a = parseValues(itemA[col.id]);
                    let b = parseValues(itemB[col.id]);
                    reduced[col.id] = +(((a + b) / 2).toFixed(2));
                }
            } else {
                for (let j = 0; j < selectedColumns.length; j++) {
                    let col = selectedColumns[j];
                    let sortedData = _.sortBy(searchObject.searchResults.data, function (item) {
                        return parseValues(item[col.id]);
                    })
                    let item = sortedData[Math.floor(searchObject.searchResults.data.length / 2)];
                    let a = parseValues(item[col.id]).toFixed(2);
                    reduced[col.id] = +(a);
                }
            }
        } else {
            for (let i = 0; i < searchObject.searchResults.data.length; i++) {
                let item = searchObject.searchResults.data[i];
                if (!groupParameters) {
                    for (let j = 0; j < selectedColumns.length; j++) {
                        let col = selectedColumns[j];
                        doReduction(reduction.id, reduced[col.id], parseValues(item[col.id]));
                    }
                } else {
                    let value;
                    for (let j = 0; j < selectedColumns.length; j++) {
                        let col = selectedColumns[j];
                        if (value !== undefined) {
                            value = value + "-" + parseValues(item[col.id]);
                        } else {
                            value = parseValues(item[col.id]);
                        }
                    }
                    doReduction(reduction.id, reduced[groupedParameterName], value);
                }
            }
        }
        if (!groupParameters) {
            let pieLoop = 1;
            if (isPieLike()) {
                // let numberOfParts = selectedColumns.length * 2;
                // let perc = (pieLoop / numberOfParts) * 100;
                chartOptions.series[0]["center"] = ["50%", "50%"];
                chartOptions.series[0]["size"] = "80%";
            }
            for (let i = 0; i < selectedColumns.length; i++) {
                let totalAllocationDrillDownKeys;
                let col = selectedColumns[i];
                switch (reduction.id) {
                    case "total_allocation":
                        if (isPieLike()) {
                            chartOptions.series[0].data.push({
                                name: col.name,
                                y: reduced[col.id][reduction.id],
                                drilldown: col.id
                            });
                        } else {
                            chartOptions.series[i].data.push({
                                name: col.name,
                                y: reduced[col.id][reduction.id],
                                drilldown: col.id
                            });

                        }
                        totalAllocationDrillDownKeys = _.keys(reduced[col.id]["total_allocation_drilldown"]);
                        let drillDownTotalAllocationPerKeySeries = [];
                        chartOptions.drilldown.series.push({
                            id: col.id,
                            name: col.name,
                            data: drillDownTotalAllocationPerKeySeries,
                            showInLegend: false
                        });
                        for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                            let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                            let y = reduced[col.id]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                            drillDownTotalAllocationPerKeySeries.push({
                                name: totalAllocationDrillDownKey,
                                y: y
                            });
                        }
                        break;
                    case "least_used_value":
                        totalAllocationDrillDownKeys = _.keys(reduced[col.id]["total_allocation_drilldown"]);
                        let leastUsedValue;
                        let leastUsedCount;
                        for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                            let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                            let y = reduced[col.id]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                            if (leastUsedCount === undefined) {
                                leastUsedCount = y;
                                leastUsedValue = parseValues(totalAllocationDrillDownKey);
                            }
                            if (y < leastUsedCount) {
                                leastUsedValue = parseValues(totalAllocationDrillDownKey);
                                leastUsedCount = y;
                            }
                        }
                        chartOptions.series[i].data.push({name: col.name, y: leastUsedValue});
                        break;
                    case "most_used_value":
                        totalAllocationDrillDownKeys = _.keys(reduced[col.id]["total_allocation_drilldown"]);
                        let mostUsedValue;
                        let mostUsedCount = 0;
                        for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                            let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                            let y = reduced[col.id]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                            if (y > mostUsedCount) {
                                mostUsedValue = parseValues(totalAllocationDrillDownKey);
                                mostUsedCount = y;
                            }
                        }
                        chartOptions.series[i].data.push({name: col.name, y: mostUsedValue});
                        break;
                    case "min":
                    case "max":
                    case "sum":
                        chartOptions.series[i].data.push({name: col.name, y: reduced[col.id][reduction.id]});
                        break;
                    case "avg":
                        chartOptions.series[i].data.push({
                            name: col.name,
                            y: +(reduced[col.id][reduction.id] / searchObject.searchResults.data.length).toFixed(2)
                        });
                        break;
                    case "median":
                        chartOptions.series[i].data.push({
                            name: col.name,
                            y: reduced[col.id]
                        });
                        break;
                    default:
                        throw Error("Unknown reduction " + reduction.id);
                }
                pieLoop = pieLoop + 2;
            }
        } else {
            let totalAllocationDrillDownKeys;
            switch (reduction.id) {
                case "total_allocation":
                    totalAllocationDrillDownKeys = _.keys(reduced[groupedParameterName]["total_allocation_drilldown"]);
                    let drillDownTotalAllocationPerKeySeries = [];
                    chartOptions.series.push({
                        name: groupedParameterName,
                        data: drillDownTotalAllocationPerKeySeries,
                        showInLegend: false
                    });
                    for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                        let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                        let y = reduced[groupedParameterName]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                        drillDownTotalAllocationPerKeySeries.push({
                            name: totalAllocationDrillDownKey,
                            y: y
                        });
                    }
                    break;
                case "least_used_value":
                    totalAllocationDrillDownKeys = _.keys(reduced[groupedParameterName]["total_allocation_drilldown"]);
                    let leastUsedValue;
                    let leastUsedCount;
                    for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                        let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                        let y = reduced[groupedParameterName]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                        if (leastUsedCount === undefined) {
                            leastUsedCount = y;
                            leastUsedValue = totalAllocationDrillDownKey;
                        }
                        if (y < leastUsedCount) {
                            leastUsedValue = totalAllocationDrillDownKey;
                            leastUsedCount = y;
                        }
                    }
                    chartOptions.series.push({
                        name: groupedParameterName,
                        data: [{name: leastUsedValue, y: leastUsedCount}]
                    });
                    break;
                case "most_used_value":
                    totalAllocationDrillDownKeys = _.keys(reduced[groupedParameterName]["total_allocation_drilldown"]);
                    let mostUsedValue;
                    let mostUsedCount = 0;
                    for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                        let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                        let y = reduced[groupedParameterName]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                        if (y > mostUsedCount) {
                            mostUsedValue = totalAllocationDrillDownKey;
                            mostUsedCount = y;
                        }
                    }
                    chartOptions.series.push({
                        name: groupedParameterName,
                        data: [{name: mostUsedValue, y: mostUsedCount}]
                    });
                    break;
                default:
                    throw Error("Grouped parameters only support Distribution, least used value and most used value. Found " + reduction.id);
            }

        }
    };


    let reduceGrouped = function () {
        let reduction = selectedReductions[0];
        let groupedReduced = {};
        chartOptions.series = [];
        chartOptions.drilldown = {series: []};

        let groupedParameterName;
        if (!groupParameters) {
            for (let i = 0; i < selectedColumns.length; i++) {
                let col = selectedColumns[i];
                groupedReduced[col.id] = {};
                chartOptions.series.push({name: col.name, data: []});
            }
        } else {
            let names = _.map(selectedColumns, function (item) {
                return item.name;
            });
            groupedParameterName = _.reduce(names, function (a, b) {
                return a + "-" + b;
            });
            groupedReduced[groupedParameterName] = {};
        }

        if (reduction.id === 'median') {
            let grouped = _.groupBy(searchObject.searchResults.data, function (item) {
                return item[selectedGroupOverColumns[0].id];
            });
            let keys = _.keys(grouped);
            for (let i = 0; i < keys.length; i++) {
                let key = keys[i];
                if (grouped[key].length % 2 === 0) {
                    for (let j = 0; j < selectedColumns.length; j++) {
                        let col = selectedColumns[j];
                        let sortedData = _.sortBy(grouped[key], function (item) {
                            return parseValues(item[col.id]);
                        })
                        let itemA = sortedData[(grouped[key].length / 2) - 1];
                        let itemB = sortedData[grouped[key].length / 2];
                        let a = parseValues(itemA[col.id]);
                        let b = parseValues(itemB[col.id]);
                        groupedReduced[col.id][key] = +(((a + b) / 2).toFixed(2));
                    }
                } else {
                    for (let j = 0; j < selectedColumns.length; j++) {
                        let col = selectedColumns[j];
                        let sortedData = _.sortBy(grouped[key], function (item) {
                            return parseValues(item[col.id]);
                        })
                        let item = sortedData[Math.floor(grouped[key].length / 2)];
                        let a = parseValues(item[col.id]).toFixed(2);
                        groupedReduced[col.id][key] = +(a);
                    }
                }
            }
        } else {
            for (let i = 0; i < searchObject.searchResults.data.length; i++) {
                let item = searchObject.searchResults.data[i];
                if (!groupParameters) {
                    for (let j = 0; j < selectedColumns.length; j++) {
                        let col = selectedColumns[j];
                        doGroupedReduction(reduction.id, selectedGroupOverColumns[0].id, groupedReduced[col.id], parseValues(item[col.id]), item[selectedGroupOverColumns[0].id]);
                    }
                } else {
                    let value;
                    for (let j = 0; j < selectedColumns.length; j++) {
                        let col = selectedColumns[j];
                        if (value !== undefined) {
                            value = value + "-" + parseValues(item[col.id]);
                        } else {
                            value = parseValues(item[col.id]);
                        }
                    }
                    doGroupedReduction(reduction.id, selectedGroupOverColumns[0].id, groupedReduced[groupedParameterName], value, item[selectedGroupOverColumns[0].id]);
                }
            }
        }

        if (!groupParameters) {
            let pieLoop = 1;
            for (let i = 0; i < selectedColumns.length; i++) {
                if (selectedChartTypes[0].id === 'pie' || selectedChartTypes[0].id === 'pie3d' || selectedChartTypes[0].id === 'donut' || selectedChartTypes[0].id === 'donut3d') {
                    let numberOfParts = selectedColumns.length * 2;
                    let perc = ((pieLoop) / numberOfParts) * 100;
                    chartOptions.series[i]["center"] = [perc + "%", "50%"];
                    chartOptions.series[i]["size"] = (100 / selectedColumns.length) + "%";
                }
                let col = selectedColumns[i];
                let groupedKeys;
                switch (reduction.id) {
                    case "total_allocation":
                        groupedKeys = _.keys(groupedReduced[col.id])
                        for (let j = 0; j < groupedKeys.length; j++) {
                            let key = groupedKeys[j];
                            chartOptions.series[i].data.push({
                                name: key,
                                y: groupedReduced[col.id][key][reduction.id],
                                drilldown: col.id + '_' + key
                            });
                            let totalAllocationDrillDownKeys = _.keys(groupedReduced[col.id][key]["total_allocation_drilldown"]);
                            let drillDownTotalAllocationPerKeySeries = [];
                            let d = {
                                id: col.id + '_' + key,
                                name: col.name + ' per ' + key,
                                data: drillDownTotalAllocationPerKeySeries,
                                showInLegend: false
                            };
                            if (isPieLike()) {
                                d.showInLegend = true;
                            }
                            chartOptions.drilldown.series.push(d);
                            for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                                let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                                let y = groupedReduced[col.id][key]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                                drillDownTotalAllocationPerKeySeries.push({
                                    name: totalAllocationDrillDownKey,
                                    y: y
                                });
                            }
                        }
                        break;
                    case "least_used_value":
                        groupedKeys = _.keys(groupedReduced[col.id])
                        for (let j = 0; j < groupedKeys.length; j++) {
                            let key = groupedKeys[j];
                            let totalAllocationDrillDownKeys = _.keys(groupedReduced[col.id][key]["total_allocation_drilldown"]);
                            let leastUsedValue;
                            let leastUsedCount;
                            for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                                let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                                let y = groupedReduced[col.id][key]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                                if (leastUsedCount === undefined) {
                                    leastUsedCount = y;
                                    leastUsedValue = parseValues(totalAllocationDrillDownKey);
                                }
                                if (y < leastUsedCount) {
                                    leastUsedValue = parseValues(totalAllocationDrillDownKey);
                                    leastUsedCount = y;
                                }
                            }
                            chartOptions.series[i].data.push({
                                name: key,
                                y: leastUsedValue
                            });
                        }
                        break;
                    case "most_used_value":
                        groupedKeys = _.keys(groupedReduced[col.id])
                        for (let j = 0; j < groupedKeys.length; j++) {
                            let key = groupedKeys[j];
                            let totalAllocationDrillDownKeys = _.keys(groupedReduced[col.id][key]["total_allocation_drilldown"]);
                            let mostUsedValue;
                            let mostUsedCount = 0;
                            for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                                let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                                let y = groupedReduced[col.id][key]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                                if (y > mostUsedCount) {
                                    mostUsedValue = parseValues(totalAllocationDrillDownKey);
                                    mostUsedCount = y;
                                }
                            }
                            chartOptions.series[i].data.push({
                                name: key,
                                y: mostUsedValue
                            });
                        }
                        break;
                    case "min":
                    case "max":
                    case "sum":
                        groupedKeys = _.keys(groupedReduced[col.id])
                        for (let j = 0; j < groupedKeys.length; j++) {
                            let key = groupedKeys[j];
                            chartOptions.series[i].data.push({
                                name: key,
                                y: groupedReduced[col.id][key][reduction.id]
                            });
                        }
                        break;
                    case "avg":
                        groupedKeys = _.keys(groupedReduced[col.id])
                        for (let j = 0; j < groupedKeys.length; j++) {
                            let key = groupedKeys[j];
                            chartOptions.series[i].data.push({
                                name: key,
                                y: +(groupedReduced[col.id][key][reduction.id] / groupedReduced[col.id][key]["avgcount"]).toFixed(2)
                            });
                        }
                        break;
                    case "median":
                        groupedKeys = _.keys(groupedReduced[col.id])
                        for (let j = 0; j < groupedKeys.length; j++) {
                            let key = groupedKeys[j];
                            chartOptions.series[i].data.push({
                                name: key,
                                y: groupedReduced[col.id][key]
                            });
                        }
                        break;
                    default:
                        throw Error("Unknown reduction " + reduction.id);
                }
                pieLoop = pieLoop + 2;
            }
        } else {
            let groupedKeys;
            switch (reduction.id) {
                case "total_allocation":
                    groupedKeys = _.keys(groupedReduced[groupedParameterName])
                    for (let j = 0; j < groupedKeys.length; j++) {
                        let key = groupedKeys[j];
                        let totalAllocationDrillDownKeys = _.keys(groupedReduced[groupedParameterName][key]["total_allocation_drilldown"]);
                        let drillDownTotalAllocationPerKeySeries = [];
                        chartOptions.series.push({
                            id: key,
                            name: key,
                            data: drillDownTotalAllocationPerKeySeries,
                            showInLegend: true
                        });
                        for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                            let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                            let y = groupedReduced[groupedParameterName][key]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                            drillDownTotalAllocationPerKeySeries.push({
                                name: totalAllocationDrillDownKey,
                                y: y
                            });
                        }
                    }
                    break;
                case "least_used_value":
                    groupedKeys = _.keys(groupedReduced[groupedParameterName])
                    for (let j = 0; j < groupedKeys.length; j++) {
                        let key = groupedKeys[j];
                        let totalAllocationDrillDownKeys = _.keys(groupedReduced[groupedParameterName][key]["total_allocation_drilldown"]);
                        let leastUsedValue;
                        let leastUsedCount;
                        for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                            let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                            let y = groupedReduced[groupedParameterName][key]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                            if (leastUsedCount === undefined) {
                                leastUsedCount = y;
                                leastUsedValue = totalAllocationDrillDownKey;
                            }
                            if (y < leastUsedCount) {
                                leastUsedValue = totalAllocationDrillDownKey;
                                leastUsedCount = y;
                            }
                        }
                        chartOptions.series.push({
                            name: key,
                            data: [{name: leastUsedValue, y: leastUsedCount}]
                        });
                    }
                    break;
                case "most_used_value":
                    groupedKeys = _.keys(groupedReduced[groupedParameterName])
                    for (let j = 0; j < groupedKeys.length; j++) {
                        let key = groupedKeys[j];
                        let totalAllocationDrillDownKeys = _.keys(groupedReduced[groupedParameterName][key]["total_allocation_drilldown"]);
                        let mostUsedValue;
                        let mostUsedCount = 0;
                        for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                            let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                            let y = groupedReduced[groupedParameterName][key]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                            if (y > mostUsedCount) {
                                mostUsedValue = totalAllocationDrillDownKey;
                                mostUsedCount = y;
                            }
                        }
                        chartOptions.series.push({
                            name: key,
                            data: [{name: mostUsedValue, y: mostUsedCount}]
                        });
                    }
                    break;
                default:
                    throw Error("Grouped parameters only support Distribution, least used value and most used value. Found " + reduction.id);
            }

        }
    };

    let reducePackedBubble = function () {
        // noinspection DuplicatedCode
        let reduction = selectedReductions[0];
        let reduced = {};
        chartOptions.series = [];
        chartOptions.drilldown = {series: []};

        if (reduction.id === "total_allocation") {
            for (let i = 0; i < selectedColumns.length; i++) {
                let col = selectedColumns[i];
                chartOptions.series.push({name: col.name, data: []});
                reduced[col.id] = {};
            }
        } else {
            chartOptions.series.push({name: "", data: []});
            for (let i = 0; i < selectedColumns.length; i++) {
                let col = selectedColumns[i];
                reduced[col.id] = {};
            }
        }

        if (reduction.id === 'median') {
            if (searchObject.searchResults.data.length % 2 === 0) {
                for (let j = 0; j < selectedColumns.length; j++) {
                    let col = selectedColumns[j];
                    let sortedData = _.sortBy(searchObject.searchResults.data, function (item) {
                        return parseValues(item[col.id]);
                    })
                    let itemA = sortedData[(searchObject.searchResults.data.length / 2) - 1];
                    let itemB = sortedData[searchObject.searchResults.data.length / 2];
                    let a = parseValues(itemA[col.id]);
                    let b = parseValues(itemB[col.id]);
                    reduced[col.id] = +(((a + b) / 2).toFixed(2));
                }
            } else {
                for (let j = 0; j < selectedColumns.length; j++) {
                    let col = selectedColumns[j];
                    let sortedData = _.sortBy(searchObject.searchResults.data, function (item) {
                        return parseValues(item[col.id]);
                    })
                    let item = sortedData[Math.floor(searchObject.searchResults.data.length / 2)];
                    let a = parseValues(item[col.id]).toFixed(2);
                    reduced[col.id] = +(a);
                }
            }
        } else {
            for (let i = 0; i < searchObject.searchResults.data.length; i++) {
                let item = searchObject.searchResults.data[i];
                for (let j = 0; j < selectedColumns.length; j++) {
                    let col = selectedColumns[j];
                    doReduction(reduction.id, reduced[col.id], parseValues(item[col.id]));
                }
            }
        }
        let pieLoop = 1;
        for (let i = 0; i < selectedColumns.length; i++) {
            let col = selectedColumns[i];
            switch (reduction.id) {
                case "total_allocation":
                    let totalAllocationDrillDownKeys = _.keys(reduced[col.id]["total_allocation_drilldown"]);
                    for (let k = 0; k < totalAllocationDrillDownKeys.length; k++) {
                        let totalAllocationDrillDownKey = totalAllocationDrillDownKeys[k];
                        let y = reduced[col.id]["total_allocation_drilldown"][totalAllocationDrillDownKey]["value"];
                        chartOptions.series[i].data.push({
                            name: totalAllocationDrillDownKey,
                            value: y
                        });
                    }
                    break;
                case "min":
                case "max":
                case "sum":
                    chartOptions.series[0].data.push({name: col.name, value: reduced[col.id][reduction.id]});
                    break;
                case "avg":
                    chartOptions.series[0].data.push({
                        name: col.name,
                        value: +(reduced[col.id][reduction.id] / searchObject.searchResults.data.length).toFixed(2)
                    });
                    break;
                case "median":
                    chartOptions.series[0].data.push({
                        name: col.name,
                        value: reduced[col.id]
                    });
                    break;
                default:
                    throw Error("Unknown reduction " + reduction.id);
            }
            pieLoop = pieLoop + 2;
        }
    };

    let doReduction = function (reduction, field, value) {
        switch (reduction) {
            case "total_allocation":
            case "least_used_value":
            case "most_used_value":
                if (field["total_allocation"] === undefined) {
                    field['total_allocation'] = 0;
                }
                if (field[value] === undefined) {
                    field[value] = value;
                    field['total_allocation'] = field['total_allocation'] + 1;
                }
                if (field['total_allocation_drilldown'] === undefined) {
                    field['total_allocation_drilldown'] = {};
                }
                if (field['total_allocation_drilldown'][value] === undefined) {
                    field['total_allocation_drilldown'][value] = {
                        value: 1
                    };
                } else {
                    field['total_allocation_drilldown'][value]["value"] = field['total_allocation_drilldown'][value]["value"] + 1;
                }
                break;
            case "min":
                if (field['min'] === undefined || value < field['min']) {
                    field['min'] = value;
                }
                break;
            case "max":
                if (field['max'] === undefined || value > field['max']) {
                    field['max'] = value;
                }
                break;
            case "sum":
                if (field["sum"] === undefined) {
                    field["sum"] = value;
                } else {
                    field["sum"] = field["sum"] + value;
                }
                break;
            case "avg":
                if (field["avg"] === undefined) {
                    field["avg"] = value;
                } else {
                    field["avg"] = field["avg"] + value;
                }
                break;
            default:
                throw Error("Unknown reduction " + reduction);
        }
    }

    let doGroupedReduction = function (reduction, groupOverColumn, field, value, groupOverValue) {
        if (field[groupOverValue] === undefined) {
            field[groupOverValue] = {"total_allocation": 0};
        }
        switch (reduction) {
            case "total_allocation":
            case "least_used_value":
            case "most_used_value":
                if (field[groupOverValue][value] === undefined) {
                    field[groupOverValue][value] = value;
                    field[groupOverValue]['total_allocation'] = field[groupOverValue]['total_allocation'] + 1;
                }
                if (field[groupOverValue]['total_allocation_drilldown'] === undefined) {
                    field[groupOverValue]['total_allocation_drilldown'] = {};
                }
                if (field[groupOverValue]['total_allocation_drilldown'][value] === undefined) {
                    field[groupOverValue]['total_allocation_drilldown'][value] = {
                        value: 1
                    };
                } else {
                    field[groupOverValue]['total_allocation_drilldown'][value]["value"] = field[groupOverValue]['total_allocation_drilldown'][value]["value"] + 1;
                }
                break;
            case "min":
                if (field[groupOverValue]['min'] === undefined || value < field[groupOverValue]['min']) {
                    field[groupOverValue]['min'] = value;
                }
                break;
            case "max":
                if (field[groupOverValue]['max'] === undefined || value > field[groupOverValue]['max']) {
                    field[groupOverValue]['max'] = value;
                }
                break;
            case "sum":
                if (field[groupOverValue]["sum"] === undefined) {
                    field[groupOverValue]["sum"] = value;
                } else {
                    field[groupOverValue]["sum"] = field[groupOverValue]["sum"] + value;
                }
                break;
            case "avg":
                if (field[groupOverValue]["avg"] === undefined) {
                    field[groupOverValue]["avg"] = value;
                    field[groupOverValue]["avgcount"] = 1;
                } else {
                    field[groupOverValue]["avg"] = field[groupOverValue]["avg"] + value;
                    field[groupOverValue]["avgcount"] = field[groupOverValue]["avgcount"] + 1;
                }
                break;
            default:
                throw Error("Unknown reduction " + reduction);
        }
    }

    let columnsFromGrid = function () {
        columns.splice(0, columns.length);
        groupOverColumns.splice(0, columns.length);
        if (searchObject !== undefined && _.has(searchObject, "searchResults")) {
            for (let i = 0; i < searchObject.searchResults.columns.length; i++) {
                let item = searchObject.searchResults.columns[i];
                if (item.name !== "" && item.name !== "#") {
                    let col = {id: item.id, name: item.name};
                    columns.push(col);
                    groupOverColumns.push(col);
                }
            }
        }
        //default from workspace
        if (workspace !== undefined && workspace.uiState !== undefined && workspace.uiState.chartConfig !== undefined) {
            //need to validate that the columns still exist.
            let chartConfig = workspace.uiState.chartConfig;
            if (chartConfig.selectedColumns !== undefined && chartConfig.selectedColumns.length > 0) {
                for (let i = 0; i < chartConfig.selectedColumns.length; i++) {
                    for (let j = 0; j < columns.length; j++) {
                        if (chartConfig.selectedColumns[i].id === columns[j].id) {
                            selectedColumns.push(chartConfig.selectedColumns[i])
                        }
                    }
                }
            }

            if (chartConfig.groupParameters !== undefined) {
                groupParameters = chartConfig.groupParameters;
            }
            if (chartConfig.selectedReductions !== undefined && chartConfig.selectedReductions.length > 0) {
                selectedReductions = chartConfig.selectedReductions;
            }
            if (chartConfig.selectedGroupOverColumns !== undefined && chartConfig.selectedGroupOverColumns.length > 0) {
                for (let i = 0; i < chartConfig.selectedGroupOverColumns.length; i++) {
                    for (let j = 0; j < columns.length; j++) {
                        if (chartConfig.selectedGroupOverColumns[i].id === columns[j].id) {
                            selectedGroupOverColumns.push(chartConfig.selectedGroupOverColumns[i])
                        }
                    }
                }
            }
            if (chartConfig.selectedChartThemes !== undefined && chartConfig.selectedChartThemes.length > 0) {
                selectedChartThemes = chartConfig.selectedChartThemes;
            }
            if (selectedColumns.length > 0) {
                setTimeout(function () {
                    showChart();
                }, 0);
            }
        }
    }

    return {
        oninit: function (vnode) {
            searchObject = vnode.attrs.searchObject;
            workspace = vnode.attrs.workspace;
            chartId = vnode.attrs.chartId;
            columnsFromGrid();
            setTimeout(function () {
                m.redraw();
            }, 0);
        },
        oncreate: function (vnode) {
        },
        onupdate: function () {
            if (!!chart) {
                showChart(); // refreshing chart ...
            }

            refreshColumns = false;
            refreshReductions = false;
            refreshGroupOverColumns = false;
            refreshChartTypes = false;
        },
        view: function (vnode) {
            searchObject = vnode.attrs.searchObject;
            workspace = vnode.attrs.workspace;
            
            if (!!vnode.attrs.searchObject.searchResults && vnode.attrs.searchObject.searchResults.columns.length > 0 && columns.length === 0) {
                columns = vnode.attrs.searchObject.searchResults.columns;
                refreshColumns = true;
            }
            
            return m("div.slick-grid-chart-root", [
                    m("div.slick-grid-chart-header", [
                        m("div.slick-grid-chart-config", [
                            m("div.form-row.bg-gray-100.mb-1", [
                                    m("div.form-group.col-md-4", [
                                            m(Label, {
                                                for: "searchAndEditChartColumn",
                                                key: "searchAndEditChartColumnLabel",
                                                text: "Select columns"
                                            }),
                                            m("div.float-right", {key: "searchAndEditChartParameterGroupKey"}, [
                                                m(Label, {
                                                    for: "searchAndEditChartParameterGroupSwitch",
                                                    key: "searchAndEditChartParameterGroupSwitchLabelKey",
                                                    text: "Group parameters"
                                                }),
                                                m(Switch, {
                                                    id: "searchAndEditChartParameterGroupSwitch",
                                                    key: "searchAndEditChartParameterGroupSwitchKey",
                                                    class: "float-right",
                                                    enabled: true,
                                                    checked: groupParameters,
                                                    toggle: function () {
                                                        groupParameters = !groupParameters;
                                                        for (let i = 0; i < reductions.length; i++) {
                                                            let r = reductions[i];
                                                            if (r.id !== "total_allocation" && r.id !== "least_used_value" && r.id !== "most_used_value") {
                                                                r.enabled = groupParameters === false;
                                                            } else {
                                                                r.enabled = true;
                                                            }
                                                        }
                                                        refreshReductions = true;
                                                    }
                                                })
                                            ]),
                                            m(SlickSelect, {
                                                id: "searchAndEditChartColumn",
                                                key: "searchAndEditChartColumnKey",
                                                placeholder: "Columns",
                                                data: columns,
                                                multiSelect: true,
                                                required: true,
                                                headerSize: 6,
                                                selectedItems: selectedColumns,
                                                refresh: refreshColumns,
                                                "data-tippy-content": "Select columns",
                                                "data-tippy-placement": "top",
                                                selectedItemCallBack: function () {
                                                    return true;
                                                }
                                            })
                                        ]
                                    ),
                                    m("div.form-group.col-md-2", [
                                            m(Label, {
                                                for: "searchAndEditChartReduction",
                                                key: "searchAndEditChartReductionLabel",
                                                text: "Options"
                                            }),
                                            m(SlickSelect, {
                                                id: "searchAndEditChartReduction",
                                                key: "searchAndEditChartReductionKey",
                                                placeholder: "Option",
                                                data: reductions,
                                                multiSelect: false,
                                                required: true,
                                                headerSize: 6,
                                                selectedItems: selectedReductions,
                                                refresh: refreshReductions,
                                                "data-tippy-content": "Select an option",
                                                "data-tippy-placement": "top",
                                                selectedItemCallBack: function () {
                                                    return true;
                                                }
                                            })
                                        ]
                                    ),
                                    m("div.form-group.col-md-2", [
                                            m(Label, {
                                                for: "searchAndEditChartReduceOverColumn",
                                                key: "searchAndEditChartReduceOverColumnLabel",
                                                text: "Grouping"
                                            }),
                                            m(SlickSelect, {
                                                id: "searchAndEditChartReduceOverColumn",
                                                key: "searchAndEditChartReduceOverColumnKey",
                                                placeholder: "Group over a column",
                                                data: groupOverColumns,
                                                multiSelect: false,
                                                required: false,
                                                headerSize: 6,
                                                selectedItems: selectedGroupOverColumns,
                                                refresh: refreshGroupOverColumns,
                                                "data-tippy-content": "Select a column to group over",
                                                "data-tippy-placement": "top",
                                                selectedItemCallBack: function () {
                                                }
                                            })
                                        ]
                                    ),
                                    m("div.form-group.col-md-2", [
                                            m(Label, {
                                                for: "searchAndEditChartChartType",
                                                key: "searchAndEditChartChartTypeLabel",
                                                text: "Chart type"
                                            }),
                                            m(SlickSelect, {
                                                id: "searchAndEditChartChartType",
                                                key: "searchAndEditChartChartTypeKey",
                                                placeholder: "Chart type",
                                                data: chartTypes,
                                                multiSelect: false,
                                                required: true,
                                                headerSize: 6,
                                                selectedItems: selectedChartTypes,
                                                refresh: refreshChartTypes,
                                                "data-tippy-content": "Select a chart type",
                                                "data-tippy-placement": "top",
                                                selectedItemCallBack: function () {
                                                }
                                            })
                                        ]
                                    ),
                                    m("div.form-group.col-md-2", [
                                        m(Label, {
                                            for: "searchAndEditChartButton",
                                            text: ""
                                        }),
                                        m("div.input-group", [
                                            m("div.input-group-append.flex-grow-1", [
                                                m(Button, {
                                                    id: "searchAndEditChartButton",
                                                    class: "btn-block w-75 btn-sm rounded-left",
                                                    text: "Chart",
                                                    icon: "fas fa-chart-bar",
                                                    'data-tippy-content': "Chart",
                                                    'data-tippy-placement': "top",
                                                    enabled: selectedColumns.length > 0,
                                                    attrs: {
                                                        style: {"background-color": getTheme(selectedChartThemes[0].id).colors[0]}
                                                    },
                                                    onclick: function () {
                                                        showChart();
                                                        if (vnode.attrs.onShowChart !== undefined) {
                                                            vnode.attrs.onShowChart({
                                                                selectedColumns: selectedColumns,
                                                                groupParameters: groupParameters,
                                                                selectedReductions: selectedReductions,
                                                                selectedGroupOverColumns: selectedGroupOverColumns,
                                                                selectedChartTypes: selectedChartTypes,
                                                                selectedChartThemes: selectedChartThemes
                                                            });
                                                        }
                                                    },
                                                }),
                                                m(Button, {
                                                    id: "searchAndEditChartThemeDropDownButton",
                                                    class: "btn-sm dropdown-toggle dropdown-toggle-split",
                                                    enabled: selectedColumns.length > 0,
                                                    'data-tippy-content': "Chart theme: " + selectedChartThemes[0].name,
                                                    'data-tippy-placement': "top",
                                                    attrs: {
                                                        "data-toggle": "dropdown",
                                                        style: {"background-color": getTheme(selectedChartThemes[0].id).colors[0]}
                                                    }
                                                }),
                                                m("div.dropdown-menu.dropdown-menu-right", [
                                                        chartThemes.map(function (theme) {
                                                            return m("button.dropdown-item", {
                                                                    onclick: function () {
                                                                        selectedChartThemes.splice(0, selectedChartThemes.length);
                                                                        selectedChartThemes.push({id: theme.id, name: theme.name});
                                                                        showChart();
                                                                        if (vnode.attrs.onShowChart !== undefined) {
                                                                            vnode.attrs.onShowChart({
                                                                                selectedColumns: selectedColumns,
                                                                                groupParameters: groupParameters,
                                                                                selectedReductions: selectedReductions,
                                                                                selectedGroupOverColumns: selectedGroupOverColumns,
                                                                                selectedChartTypes: selectedChartTypes,
                                                                                selectedChartThemes: selectedChartThemes
                                                                            });
                                                                        }
                                                                    }
                                                                },
                                                                m('i.fas.fa-paint-brush-alt', {style: {color: getTheme(theme.id).colors[0]}}), " " + theme.name
                                                            );
                                                        })
                                                    ]
                                                )
                                            ])
                                        ])
                                    ])
                                ]
                            )
                        ])
                    ]),
                    m("div.slick-grid-chart-wrapper",
                        m("div.slick-grid-chart",
                            m("div.slick-grid-chart-container", {id: chartId}),
                        )
                    )
                ]
            );
        }
    }

}

export default SlickChart;
