import m from 'mithril';

Highcharts.theme = {
	lang: {
		drillUpText: "<< Back"
    },
    colors: ['#7cb5ec', '#434348', '#90ed7d', '#f7a35c', '#8085e9', '#f15c80', '#e4d354', '#8085e8', '#8d4653', '#91e8e1'],
    chart: {
		color: '#FFFFFF',
		borderWidth: 0,
		plotBackgroundColor: 'rgba(255, 255, 255, .9)',
		plotShadow: true,
		plotBorderWidth: 1
	}
};
Highcharts.setOptions(Highcharts.theme);

let initChart = function(){

    this.build = function(chartData, pieData, selection){

        let seriesData = [];
        pieData.data.map(function(p){
            seriesData.push([p.Vendor,parseInt(p[selection])]);
        });

        pieData.data.slice(0,1);

       new Highcharts.chart(chartData, {
            chart: {
                type: "pie",
                spacing: [0, 0, 0, 0],
                margin: [0, 0, -50, 0],
                width: 500,
                height: 200,
                plotBackgroundColor: 'rgba(255, 255, 255)',
                plotBorderWidth: 0,
                plotShadow: false
            },
            title: {
                text: selection,
                align: "center",
                verticalAlign: "middle",
                y: 72,
                style: {color: "#808080"}
            },
            tooltip: {
                pointFormat: '{series.name}: <b>{point.percentage:.1f}%</b>'
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
                        format: '<b>{point.name}</b>: {point.percentage:.1f} %',
                        style: {
                            color: (Highcharts.theme && Highcharts.theme.contrastTextColor) || "black"
                        }
                    }
                }
            },
            series: [{
                type: 'pie',
                name: 'Browser share',
                innerSize: '50%',
                data: seriesData
            }]
        });

    };
    

};

const Pie = {

    oninit: function(vnode){

        let createChart = new initChart();
        vnode.state.createChart = createChart;

     },

     oncreate: function(vnode){

     },

     onupdate: function(vnode){
   
     },

     view: function(vnode){
        let parentDom = document.getElementById(vnode.attrs.gridName);
        if(parentDom != null){
            let selection = vnode.attrs.changed;
            let pieData = {
                data: vnode.attrs.data.data,
                selection: selection
            };
            vnode.state.createChart.build(parentDom, pieData, selection);   
        } 
        return m('div', {id:vnode.attrs.gridName});
     }
};

export default Pie;