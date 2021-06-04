import m from 'mithril';

let selectedState = {};
let selectedMO;
let selectedType;
let graphTypes = [
    {
        label: "Spline",
        value : "spline"
    },
    {
        label: "Area",
        value : "area"
    },
    {
        label: "Bar",
        value : "bar"
    },
    {
        label: "Column",
        value : "column"
    }
];

let initChart = function(){

    this.build = function(chartData, report, moValue, chartType){

        function mutilator(obj, name = "mutilated", context = window) {
            const mutilated = {};
            for (let prop in obj) {
              let ref = `m-${prop}`;
              mutilated[ref] = obj[prop];
              Object.defineProperty(mutilated, prop, {
                set: function(v) {
                  this[ref] = v;
                  context.dispatchEvent(
                    new CustomEvent(`${name}:${prop}`, {
                      detail: { prop: prop, value: v }
                    })
                  );
                },
                get: function() {
                  return this[ref];
                }
              });
            }
            return mutilated;
          }


          let defaultMoValue;
          let defaultChartType;

          if(!selectedState[report.subgroupName]){

            defaultMoValue = report.selected[1];
            defaultChartType = "spline";

            moValue = defaultMoValue;
            chartType = defaultChartType; 

            selectedState[report.subgroupName] = mutilator({
                selected: defaultMoValue,
                type: defaultChartType
              });
          }

          if(selectedState[report.subgroupName].selected != moValue && typeof moValue === "string" ){
            selectedState[report.subgroupName].selected = moValue;
          }

          if(selectedState[report.subgroupName].type != chartType && typeof chartType === "string"){
            selectedState[report.subgroupName].type = chartType;
          } 


        let opt = {
            background: {
                linearGradient: [0, 0, 0, 1000],
                stops: [[0, 'rgb(255, 255, 255)'], [1, 'rgb(200, 200, 255)']]
            },
            legend: {
                layout: "vertical",
                align: "right",
                verticalAlign: "middle",
                borderWidth: 0,
                itemStyle: {
                    "color": "#808080",
                    "cursor": "pointer",
                    "fontSize": "12px",
                    "fontWeight": "bold"
                }
            },
            xAxis: {
                gridLineWidth: 0,
                minorGridLineWidth: 0,
                lineColor: '#c0c0c0',
                tickColor: '#c0c0c0',
                tickWidth: 1,
                type: 'datetime',
                dateTimeLabelFormats: {
                    month: '%e. %b',
                    year: '%b',
                    gapGridLineWidth: 0
                },
                title: false
            },
            yAxis: {
                gridLineWidth: 1,
                gridLineColor: '#c0c0c0',
                minorGridLineWidth: 0,
                lineWidth: 0,
                lineColor: '#c0c0c0',
                tickColor: '#c0c0c0',
                tickWidth: 1,
                title: {text: "Value"}, plotLines: [{value: 0, width: 1, color: "#C0C0C0"}]
            },
            tooltip: {
                shared: true,
                crosshairs: true,
                pointFormat: '<span style="color:{series.color}">+</span> {series.name}: <b>{point.y}</b><br/>'
            },
            plotOptions: {
                series: {stacking: null, connectNulls: true}
            }
        };

        new Highcharts.chart(chartData, {
                colors: ['#7cb5ec', '#434348', '#90ed7d', '#f7a35c',
                '#8085e9', '#f15c80', '#e4d354', '#8085e8', '#8d4653', '#91e8e1'],
                chart: {
                    type: selectedState[report.subgroupName].type,
                    zoomType: "x",
                    margin: [20, 150, 50, 70],
                    background: opt.background,
                    borderWidth: 0,
                    plotBackgroundColor: false,
                    plotShadow: false,
                    plotBorderWidth: 0,
                    // width: (window.innerWidth > document.documentElement.clientWidth) ? (window.outerWidth - 40) : (window.outerWidth - 32),
                    height:300
                },
                exporting: {
                    buttons: {
                      contextButton: {
                        menuItems: [
                          'printChart',
                          'separator',
                          'downloadPNG',
                          'downloadJPEG',
                          'downloadPDF',
                          'downloadSVG',
                        ]
                      }
                    }
                
                  },
                title: false,
                credits: {
                    enabled: false
                },
                legend: opt.legend,
                xAxis: opt.xAxis,
                yAxis: opt.yAxis,
                tooltip: opt.tooltip,
                plotOptions: opt.plotOptions,
                series: report.chartData[selectedState[report.subgroupName].selected]
        });
        
    };

    this.getSelection = function(){
        return selectedState;
    };

};

let parentDom;

const Chart = {

    oninit: function(vnode){
        let createChart = new initChart();
        vnode.state.createChart = createChart;
     },

     oncreate: function(vnode){
        parentDom = document.getElementById('Chart');
        if(parentDom != null){
            vnode.state.createChart.build(parentDom, vnode.attrs);   
        }
     },

     view: function(vnode){
        if(parentDom != null){
            vnode.state.createChart.build(parentDom, vnode.attrs);   
        }
        vnode.state.testThing = vnode.state.createChart.getSelection();
        return m('div', { class:"historyChart" }, [
            m('div.chartTools', [
            m('select.form-control.tool', {onchange : function(){ 
                selectedMO = this.value;

                let parentDomPie = document.getElementById(vnode.attrs.subgroupName);
                vnode.attrs.pie.build(parentDomPie, vnode.attrs.selectedReport, selectedMO);

                let parentDom = document.getElementById('Chart');  
                vnode.state.createChart.build(parentDom, vnode.attrs, selectedMO, selectedType); 
                selectedMO = undefined; 
                selectedType = undefined;
                }
            }, [
                vnode.attrs.vdata.map(function(item){
                    return m('option', {
                        value: item.id,
                        selected: (vnode.state.testThing[vnode.attrs.subgroupName] && vnode.state.testThing[vnode.attrs.subgroupName].selected === item.id) ? "selected" : ""
                    }, item.label);
                })
            ]),   
            
            m('select.form-control.tool', {
                onchange : function(){ 
                    selectedType = this.value;
                    let parentDom = document.getElementById('Chart');  
                    vnode.state.createChart.build(parentDom, vnode.attrs, selectedMO, selectedType);  
                    selectedMO = undefined; 
                    selectedType = undefined;
                    }
            }, [
                graphTypes.map(function(gtype){
                    return m('option', {
                        value: gtype.value,
                        selected: (vnode.state.testThing[vnode.attrs.subgroupName] && vnode.state.testThing[vnode.attrs.subgroupName].type === gtype.value) ? "selected" : ""
                    }, gtype.label);
                })
            ]), 
            ]),
            m('div', {id:'Chart'}),
        ]);
     }
};

export default Chart;