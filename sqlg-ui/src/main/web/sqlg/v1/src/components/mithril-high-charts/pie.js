import m from 'mithril';

let selectedState;
let chartTypes = [
    {
        label: "Pie 3D",
        value : "pie3d",
        icon : "fas fa-chart-pie"
    },
    {
        label: "Column",
        value : "column",
        icon : "fas fa-chart-bar"
    },
    {
        label: "Doughnut",
        value : "pie",
        icon : "fab fa-cuttlefish"
    },
];

let initPieChart = function(){

    selectedState = selectedState;

    this.build = function(chartData, report, initialValue, type){

        if(!report.now){
           return;
        }

        let seriesData = [];
        let selected = {};
        let vdata = [];
        let rootComparison= report.now.grid.columns[0].field; 

        if(report.now.grid.data.length == 0){
            return;
        }

        report.now.grid.columns.map(function(p){
            selected[p.field] = [];
            selected[p.field].push(p.field);
            vdata.push({
                id: p.field,
                label: p.name
            });
        });

        if(initialValue === undefined){
            initialValue = '';
            initialValue = report.now.grid.columns[1].field;
            selectedState = initialValue;
        }else{
            selectedState = initialValue;
        }

        report.now.grid.data.map(function(p){
            seriesData.push([p[rootComparison],parseInt(p[selected[initialValue][0]])]);
        });

        new Highcharts.chart(chartData, {
            colors: ['#7cb5ec', '#434348', '#90ed7d', '#f7a35c',
		    '#8085e9', '#f15c80', '#e4d354', '#8085e8', '#8d4653', '#91e8e1'],
            chart: {
                width: 600,
                height: 300,
                plotBackgroundColor: '#fff',
                plotBorderWidth: 0,
                plotShadow: false,
                backgroundColor: 'rgb(255, 255, 255)',
                borderWidth:0
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
            title: {
                text: selected[initialValue].toString().replace(/_/g, ' ')+`<br>`+report.subgroupName,
                align: "center",
                verticalAlign: "middle",
                y: 72,
                style: {color: "#808080"}
            },
            tooltip: {
                formatter: function() {
                    if ( this.y > 1000000 ) {
                        return `<b>${this.point.name}</b><br/>${this.series.name}: ${Highcharts.numberFormat( this.y/1000000, 1)} m`;
                    }else if((this.y > 1000) && (this.y < 999999) ){
                        return `<b>${this.point.name}</b><br/>${this.series.name}: ${Highcharts.numberFormat( this.y/1000, 1)} k`;
                    }else {
                        return `<b>${this.point.name}</b><br/>${this.series.name}: ${Highcharts.numberFormat( this.y, 0)}`;
                    }
                }
            },
            credits: {
                enabled: false
            },
            plotOptions: {
                pie: {
                    size: "110%",
                    startAngle: -90,
                    endAngle: 90,
                    center: ['50%', '75%'],
                    allowPointSelect: true,
                    cursor: "pointer",
                    dataLabels: {
                        enabled: true,
                        formatter: function() {
                            if ( this.y > 1000000 ) {
                                return `<b>${this.point.name}</b>: ${Highcharts.numberFormat( this.y/1000000, 1)}m`;
                            }else if((this.y > 1000) && (this.y < 999999) ){
                                return `<b>${this.point.name}</b>: ${Highcharts.numberFormat( this.y/1000, 1)}k`;
                            }else {
                                return `<b>${this.point.name}</b>: ${Highcharts.numberFormat(this.y,0)}`;
                            }
                        }, 
                        style: {
                            color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || "black"
                        }
                    }
                }
            },
            series: [{
                type: 'pie',
                name: selected[initialValue],
                innerSize: '60%',
                data: seriesData
            }]
        });

    };
};

const Pie = {

    oninit: function(vnode){
        let createPieChart = new initPieChart();
        vnode.state.createPieChart = createPieChart;
        vnode.attrs.createPieChart = createPieChart;
     },

     oncreate: function(vnode){
        let initialValue; 
        let parentDom = document.getElementById(vnode.attrs.reportId);
        if(parentDom != null){
            vnode.state.createPieChart.build(parentDom, vnode.attrs, initialValue);   
        }
     },

     view: function(vnode){
        if(!vnode.attrs.now){
            return;
        }else {
            return m('div.mainChart', [
                m('div.mainChartTools', [
                    chartTypes.map(function(ctype){
                        return m('div.ctype', {
                            class:ctype.value,
                            "data-type" : ctype.value,
                            title : ctype.label,
                            onclick: function(){

                                let initialValue;
                                let parentDom = document.getElementById(vnode.attrs.reportId);
                                vnode.state.createPieChart.build(parentDom, vnode.attrs, initialValue, $(this).data('type'));
                                $(this).toggleClass('selected');
                            },
                        }, [
                            m('i', {
                                id: ctype.value,
                                class:ctype.icon,
                            })
                        ]);
                    })
                ]),
                m('div', {id:vnode.attrs.reportId})
             ]);
        }

     }
};

export default Pie;