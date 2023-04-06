import React from 'react';
import ReactApexChart from "react-apexcharts";

import getChartColorsArray from "../../../../Components/Common/ChartsDynamicColor";

const BasicColumn = ({dataColors}) => {
    var chartColumnColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Net Profit",
            data: [46, 57, 59, 54, 62, 58, 64, 60, 66],
        },
        {
            name: "Revenue",
            data: [74, 83, 102, 97, 86, 106, 93, 114, 94],
        },
        {
            name: "Free Cash Flow",
            data: [37, 42, 38, 26, 47, 50, 54, 55, 43],
        },
    ];

    var options = {
        chart: {
            height: 350,
            type: 'bar',
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                horizontal: false,
                columnWidth: '45%',
                endingShape: 'rounded'
            },
        },
        dataLabels: {
            enabled: false
        },
        stroke: {
            show: true,
            width: 2,
            colors: ['transparent']
        },
        colors: chartColumnColors,
        xaxis: {
            categories: ['Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct'],
        },
        yaxis: {
            title: {
                text: '$ (thousands)'
            }
        },
        grid: {
            borderColor: '#f1f1f1',
        },
        fill: {
            opacity: 1

        },
        tooltip: {
            y: {
                formatter: function (val) {
                    return "$ " + val + " thousands";
                }
            }
        }
    };


    return (
        <ReactApexChart dir="ltr" className="apex-charts"
            series={series}
            options={options}
            type="bar"
            height={350}
        />
    );
};

const ColumnWithLable = ({dataColors}) => {
    var chartColumnDatatalabelColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Inflation",
            data: [2.5, 3.2, 5.0, 10.1, 4.2, 3.8, 3, 2.4, 4.0, 1.2, 3.5, 0.8],
        },
    ];

    const options = {
        chart: {
            toolbar: {
                show: !1,
            },
        },
        plotOptions: {
            bar: {
                dataLabels: {
                    position: "top", // top, center, bottom
                },
            },
        },
        dataLabels: {
            enabled: !0,
            formatter: function (val) {
                return val + "%";
            },
            offsetY: -20,
            style: {
                fontSize: "12px",
                colors: ["#adb5bd"],
            },
        },
        colors: chartColumnDatatalabelColors,
        grid: {
            borderColor: "#f1f1f1",
        },
        xaxis: {
            categories: [
                "Jan",
                "Feb",
                "Mar",
                "Apr",
                "May",
                "Jun",
                "Jul",
                "Aug",
                "Sep",
                "Oct",
                "Nov",
                "Dec",
            ],
            position: "top",
            labels: {
                offsetY: -18,
            },
            axisBorder: {
                show: !1,
            },
            axisTicks: {
                show: !1,
            },
            crosshairs: {
                fill: {
                    type: "gradient",
                    gradient: {
                        colorFrom: "#D8E3F0",
                        colorTo: "#BED1E6",
                        stops: [0, 100],
                        opacityFrom: 0.4,
                        opacityTo: 0.5,
                    },
                },
            },
            tooltip: {
                enabled: !0,
                offsetY: -35,
            },
        },
        fill: {
            gradient: {
                shade: "light",
                type: "horizontal",
                shadeIntensity: 0.25,
                gradientToColors: undefined,
                inverseColors: !0,
                opacityFrom: 1,
                opacityTo: 1,
                stops: [50, 0, 100, 100],
            },
        },
        yaxis: {
            axisBorder: {
                show: !1,
            },
            axisTicks: {
                show: !1,
            },
            labels: {
                show: !1,
                formatter: function (val) {
                    return val + "%";
                },
            },
        },
        title: {
            text: "Monthly Inflation in Argentina, 2002",
            floating: !0,
            offsetY: 320,
            align: "center",
            style: {
                color: "#444",
                fontWeight: 500,
            },
        },
    };

    return (
        <ReactApexChart dir="ltr" className="apex-charts" series={series} options={options} type="bar" height={350} />
    );
};

const StackedColumn = ({dataColors}) => {
    var chartColumnStackedColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "PRODUCT A",
            data: [44, 55, 41, 67, 22, 43],
        },
        {
            name: "PRODUCT B",
            data: [13, 23, 20, 8, 13, 27],
        },
        {
            name: "PRODUCT C",
            data: [11, 17, 15, 15, 21, 14],
        },
        {
            name: "PRODUCT D",
            data: [21, 7, 25, 13, 22, 8],
        },
    ];

    const options = {
        chart: {
            stacked: !0,
            toolbar: {
                show: !1,
            },
            zoom: {
                enabled: !0,
            },
        },
        responsive: [
            {
                breakpoint: 480,
                options: {
                    legend: {
                        position: "bottom",
                        offsetX: -10,
                        offsetY: 0,
                    },
                },
            },
        ],
        plotOptions: {
            bar: {
                horizontal: !1,
                borderRadius: 10,
            },
        },
        xaxis: {
            type: "datetime",
            categories: [
                "01/01/2011 GMT",
                "01/02/2011 GMT",
                "01/03/2011 GMT",
                "01/04/2011 GMT",
                "01/05/2011 GMT",
                "01/06/2011 GMT",
            ],
        },
        legend: {
            position: "right",
            offsetY: 40,
        },
        fill: {
            opacity: 1,
        },
        colors: chartColumnStackedColors,
    };
    return <ReactApexChart dir="ltr" className="apex-charts" series={series} options={options} type="bar" height={350} />;
};

const StackedColumn2 = ({dataColors}) => {
    var chartColumnStacked100Colors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "PRODUCT A",
            data: [44, 55, 41, 67, 22, 43, 21, 49],
        },
        {
            name: "PRODUCT B",
            data: [13, 23, 20, 8, 13, 27, 33, 12],
        },
        {
            name: "PRODUCT C",
            data: [11, 17, 15, 15, 21, 14, 15, 13],
        },
    ];

    const options = {

        chart: {
            stacked: !0,
            stackType: "100%",
            toolbar: {
                show: !1,
            },
        },
        responsive: [
            {
                breakpoint: 480,
                options: {
                    legend: {
                        position: "bottom",
                        offsetX: -10,
                        offsetY: 0,
                    },
                },
            },
        ],
        xaxis: {
            categories: [
                "2011 Q1",
                "2011 Q2",
                "2011 Q3",
                "2011 Q4",
                "2012 Q1",
                "2012 Q2",
                "2012 Q3",
                "2012 Q4",
            ],
        },
        fill: {
            opacity: 1,
        },
        legend: {
            position: "right",
            offsetX: 0,
            offsetY: 50,
        },
        colors: chartColumnStacked100Colors,
    };

    return <ReactApexChart dir="ltr" className="apex-charts" series={series} options={options} type="bar" height={350} />;
};

const ColumnMarker = ({dataColors}) => {
    var chartColumnMarkersColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Actual",
            data: [
                {
                    x: "2011",
                    y: 1292,
                    goals: [
                        {
                            name: "Expected",
                            value: 1400,
                            strokeWidth: 5,
                            strokeColor: "#775DD0",
                        },
                    ],
                },
                {
                    x: "2012",
                    y: 4432,
                    goals: [
                        {
                            name: "Expected",
                            value: 5400,
                            strokeWidth: 5,
                            strokeColor: "#775DD0",
                        },
                    ],
                },
                {
                    x: "2013",
                    y: 5423,
                    goals: [
                        {
                            name: "Expected",
                            value: 5200,
                            strokeWidth: 5,
                            strokeColor: "#775DD0",
                        },
                    ],
                },
                {
                    x: "2014",
                    y: 6653,
                    goals: [
                        {
                            name: "Expected",
                            value: 6500,
                            strokeWidth: 5,
                            strokeColor: "#775DD0",
                        },
                    ],
                },
                {
                    x: "2015",
                    y: 8133,
                    goals: [
                        {
                            name: "Expected",
                            value: 6600,
                            strokeWidth: 5,
                            strokeColor: "#775DD0",
                        },
                    ],
                },
                {
                    x: "2016",
                    y: 7132,
                    goals: [
                        {
                            name: "Expected",
                            value: 7500,
                            strokeWidth: 5,
                            strokeColor: "#775DD0",
                        },
                    ],
                },
                {
                    x: "2017",
                    y: 7332,
                    goals: [
                        {
                            name: "Expected",
                            value: 8700,
                            strokeWidth: 5,
                            strokeColor: "#775DD0",
                        },
                    ],
                },
                {
                    x: "2018",
                    y: 6553,
                    goals: [
                        {
                            name: "Expected",
                            value: 7300,
                            strokeWidth: 5,
                            strokeColor: "#775DD0",
                        },
                    ],
                },
            ],
        },
    ];
    const options = {
        chart: {
            toolbar: {
                show: !1,
            },
        },
        plotOptions: {
            bar: {
                columnWidth: "30%",
            },
        },
        colors: chartColumnMarkersColors,
        dataLabels: {
            enabled: !1,
        },
        legend: {
            show: !0,
            showForSingleSeries: !0,
            customLegendItems: ["Actual", "Expected"],
            markers: {
                fillColors: ["#51d28c", "#564ab1"],
            },
        },
    };
    return (
        <ReactApexChart dir="ltr" className="apex-charts" options={options} series={series} type="bar" height={350} />
    );
};

const RotateLable = ({dataColors}) => {
    var chartColumnRotateLabelsColors = getChartColorsArray(dataColors);
    const series = [{
        name: 'Servings',
        data: [44, 55, 41, 67, 22, 43, 21, 33, 45, 31, 87, 65, 35]
    }];
    var options = {
        annotations: {
            points: [{
                x: 'Bananas',
                seriesIndex: 0,
                label: {
                    borderColor: '#775DD0',
                    offsetY: 0,
                    style: {
                        color: '#fff',
                        background: '#775DD0',
                    },
                    text: 'Bananas are good',
                }
            }]
        },
        chart: {
            height: 350,
            type: 'bar',
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                borderRadius: 10,
                columnWidth: '50%',
            }
        },
        dataLabels: {
            enabled: false
        },
        stroke: {
            width: 2
        },
        colors: chartColumnRotateLabelsColors,
        xaxis: {
            labels: {
                rotate: -45
            },
            categories: ['Apples', 'Oranges', 'Strawberries', 'Pineapples', 'Mangoes', 'Bananas',
                'Blackberries', 'Pears', 'Watermelons', 'Cherries', 'Pomegranates', 'Tangerines', 'Papayas'
            ],
            tickPlacement: 'on'
        },
        yaxis: {
            title: {
                text: 'Servings',
            },
        },
        fill: {
            type: 'gradient',
            gradient: {
                shade: 'light',
                type: "horizontal",
                shadeIntensity: 0.25,
                gradientToColors: undefined,
                inverseColors: true,
                opacityFrom: 0.85,
                opacityTo: 0.85,
                stops: [50, 0, 100]
            },
        }
    };

    return (
        <ReactApexChart dir="ltr" className="apex-charts" options={options} series={series} type="bar" height={350} />
    );
};

const NagetiveLable = ({dataColors}) => {
    var chartNagetiveValuesColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Cash Flow",
            data: [
                1.45, 5.42, 5.9, -0.42, -12.6, -18.1, -18.2, -14.16, -11.1, -6.09, 0.34,
                3.88, 13.07, 5.8, 2, 7.37, 8.1, 13.57, 15.75, 17.1, 19.8, -27.03, -54.4,
                -47.2, -43.3, -18.6, -48.6, -41.1, -39.6, -37.6, -29.4, -21.4, -2.4,
            ],
        },
    ];
    var options = {
        chart: {
            type: 'bar',
            height: 350,
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                colors: {
                    ranges: [{
                        from: -100,
                        to: -46,
                        color: chartNagetiveValuesColors[1]
                    }, {
                        from: -45,
                        to: 0,
                        color: chartNagetiveValuesColors[2]
                    }]
                },
                columnWidth: '80%',
            }
        },
        dataLabels: {
            enabled: false,
        },
        colors: chartNagetiveValuesColors[0],
        yaxis: {
            title: {
                text: 'Growth',
            },
            labels: {
                formatter: function(y) {
                    return y.toFixed(0) + "%";
                }
            }
        },
        xaxis: {
            type: 'datetime',
            categories: [
                '2011-01-01', '2011-02-01', '2011-03-01', '2011-04-01', '2011-05-01', '2011-06-01',
                '2011-07-01', '2011-08-01', '2011-09-01', '2011-10-01', '2011-11-01', '2011-12-01',
                '2012-01-01', '2012-02-01', '2012-03-01', '2012-04-01', '2012-05-01', '2012-06-01',
                '2012-07-01', '2012-08-01', '2012-09-01', '2012-10-01', '2012-11-01', '2012-12-01',
                '2013-01-01', '2013-02-01', '2013-03-01', '2013-04-01', '2013-05-01', '2013-06-01',
                '2013-07-01', '2013-08-01', '2013-09-01'
            ],
            labels: {
                rotate: -90
            },
        },
    };

    return (
        <ReactApexChart dir="ltr" className="apex-charts" series={series} options={options} type="bar" height={350} />
    );
};

const RangeColumn = ({dataColors}) => {
    var chartRangeColors = getChartColorsArray(dataColors);
    const series = [
        {
            data: [
                {
                    x: "Team A",
                    y: [1, 5],
                },
                {
                    x: "Team B",
                    y: [4, 6],
                },
                {
                    x: "Team C",
                    y: [5, 8],
                },
                {
                    x: "Team D",
                    y: [3, 11],
                },
            ],
        },
        {
            data: [
                {
                    x: "Team A",
                    y: [2, 6],
                },
                {
                    x: "Team B",
                    y: [1, 3],
                },
                {
                    x: "Team C",
                    y: [7, 8],
                },
                {
                    x: "Team D",
                    y: [5, 9],
                },
            ],
        },
    ];
    var options = {
        chart: {
            type: "rangeBar",
            height: 335,
            toolbar: {
                show: false,
            },
        },
        plotOptions: {
            bar: {
                horizontal: !1,
            },
        },
        dataLabels: {
            enabled: !0,
        },
        legend: {
            show: !1,
        },
        colors: chartRangeColors,
    };

    return (
        <ReactApexChart dir="ltr" className="apex-charts"
            options={options}
            series={series}
            type="rangeBar"
            height={335}
        />
    );
};

const DynamicColumn = ({dataColors}) => {
    var chartColumnColors = getChartColorsArray(dataColors);
    const colors = chartColumnColors;
    const shuffleArray = (array1) => {
        for (var i = array1.length - 1; i > 0; i--) {
            var j = Math.floor(Math.random() * (i + 1));
            var temp = array1[i];
            array1[i] = array1[j];
            array1[j] = temp;
        }
        return array1;
    };

    const arrayData = [
        {
            y: 400,
            quarters: [
                {
                    x: "Q1",
                    y: 120,
                },
                {
                    x: "Q2",
                    y: 90,
                },
                {
                    x: "Q3",
                    y: 100,
                },
                {
                    x: "Q4",
                    y: 90,
                },
            ],
        },
        {
            y: 430,
            quarters: [
                {
                    x: "Q1",
                    y: 120,
                },
                {
                    x: "Q2",
                    y: 110,
                },
                {
                    x: "Q3",
                    y: 90,
                },
                {
                    x: "Q4",
                    y: 110,
                },
            ],
        },
        {
            y: 448,
            quarters: [
                {
                    x: "Q1",
                    y: 70,
                },
                {
                    x: "Q2",
                    y: 100,
                },
                {
                    x: "Q3",
                    y: 140,
                },
                {
                    x: "Q4",
                    y: 138,
                },
            ],
        },
        {
            y: 470,
            quarters: [
                {
                    x: "Q1",
                    y: 150,
                },
                {
                    x: "Q2",
                    y: 60,
                },
                {
                    x: "Q3",
                    y: 190,
                },
                {
                    x: "Q4",
                    y: 70,
                },
            ],
        },
        {
            y: 540,
            quarters: [
                {
                    x: "Q1",
                    y: 120,
                },
                {
                    x: "Q2",
                    y: 120,
                },
                {
                    x: "Q3",
                    y: 130,
                },
                {
                    x: "Q4",
                    y: 170,
                },
            ],
        },
        {
            y: 580,
            quarters: [
                {
                    x: "Q1",
                    y: 170,
                },
                {
                    x: "Q2",
                    y: 130,
                },
                {
                    x: "Q3",
                    y: 120,
                },
                {
                    x: "Q4",
                    y: 160,
                },
            ],
        },
    ];

    const makeData = () => {
        var dataSet = shuffleArray(arrayData);

        var dataYearSeries = [
            {
                x: "2011",
                y: dataSet[0].y,
                color: colors[0],
                quarters: dataSet[0].quarters,
            },
            {
                x: "2012",
                y: dataSet[1].y,
                color: colors[1],
                quarters: dataSet[1].quarters,
            },
            {
                x: "2013",
                y: dataSet[2].y,
                color: colors[2],
                quarters: dataSet[2].quarters,
            },
            {
                x: "2014",
                y: dataSet[3].y,
                color: colors[3],
                quarters: dataSet[3].quarters,
            },
            {
                x: "2015",
                y: dataSet[4].y,
                color: colors[4],
                quarters: dataSet[4].quarters,
            },
            {
                x: "2016",
                y: dataSet[5].y,
                color: colors[5],
                quarters: dataSet[5].quarters,
            },
        ];

        return dataYearSeries;
    };

    const updateQuarterChart = (sourceChart, destChartIDToUpdate) => {
        var series = [];
        var seriesIndex = 0;
        var colors = [];

        if (sourceChart.w.globals.selectedDataPoints[0]) {
            var selectedPoints = sourceChart.w.globals.selectedDataPoints;
            for (var i = 0; i < selectedPoints[seriesIndex].length; i++) {
                var selectedIndex = selectedPoints[seriesIndex][i];
                var yearSeries = sourceChart.w.config.series[seriesIndex];
                series.push({
                    name: yearSeries.data[selectedIndex].x,
                    data: yearSeries.data[selectedIndex].quarters,
                });
                colors.push(yearSeries.data[selectedIndex].color);
            }

            if (series.length === 0)
                series = [
                    {
                        data: [],
                    },
                ];

            return window.ApexCharts.exec(destChartIDToUpdate, "updateOptions", {
                series: series,
                colors: colors,
                fill: {
                    colors: colors,
                },
            });
        }
    };

    const series = [
        {
            data: makeData(),
        },
    ];

    const options = {

        chart: {
            id: "barYear",
            height: 330,
            width: "100%",
            events: {
                dataPointSelection: function (e, chart, opts) {
                    const quarterChartEl = document.querySelector("#chart-quarter");
                    const yearChartEl = document.querySelector("#chart-year");

                    if (opts.selectedDataPoints[0].length === 1) {
                        if (quarterChartEl) {
                            if (quarterChartEl.classList.contains("active")) {
                                updateQuarterChart(chart, "barQuarter");
                            } else {
                                if (yearChartEl) {
                                    yearChartEl.classList.add("chart-quarter-activated");
                                }
                                quarterChartEl.classList.add("active");
                                updateQuarterChart(chart, "barQuarter");
                            }
                        }
                    } else {
                        updateQuarterChart(chart, "barQuarter");
                    }

                    if (opts.selectedDataPoints[0].length === 0) {
                        if (yearChartEl) {
                            yearChartEl.classList.remove("chart-quarter-activated");
                        }
                        if (quarterChartEl) {
                            quarterChartEl.classList.remove("active");
                        }
                    }
                },
                updated: function (chart) {
                    updateQuarterChart(chart, "barQuarter");
                },
            },
        },
        plotOptions: {
            bar: {
                distributed: !0,
                horizontal: !0,
                barHeight: "75%",
                dataLabels: {
                    position: "bottom",
                },
            },
        },
        dataLabels: {
            enabled: !0,
            textAnchor: "start",
            style: {
                colors: ["#fff"],
            },
            formatter: function (val, opt) {
                return opt.w.globals.labels[opt.dataPointIndex];
            },
            offsetX: 0,
            dropShadow: {
                enabled: !1,
            },
        },

        colors: colors,

        states: {
            normal: {
                filter: {
                    type: "desaturate",
                },
            },
            active: {
                allowMultipleDataPointsSelection: !0,
                filter: {
                    type: "darken",
                    value: 1,
                },
            },
        },
        tooltip: {
            x: {
                show: !1,
            },
            y: {
                title: {
                    formatter: function (val, opts) {
                        return opts.w.globals.labels[opts.dataPointIndex];
                    },
                },
            },
        },
        title: {
            text: "Yearly Results",
            offsetX: 15,
            style: {
                fontWeight: 600,
            },
        },
        subtitle: {
            text: "(Click on bar to see details)",
            offsetX: 15,
        },
        yaxis: {
            labels: {
                show: !1,
            },
        },
    };

    return <ReactApexChart dir="ltr" className="apex-charts" series={series} options={options} type="bar" height={330} />;
};


const Quarter = ({ dataColors }) => {
    var ChartQuater = getChartColorsArray(dataColors);
    const series = [
        {
            data: [],
        },
    ];

    var optionsQuarter = {
        chart: {
            id: 'barQuarter',
            height: 330,
            width: '100%',
            type: 'bar',
            stacked: true
        },
        plotOptions: {
            bar: {
                columnWidth: '50%',
                horizontal: false
            }
        },
        legend: {
            show: false
        },
        grid: {
            yaxis: {
                lines: {
                    show: false,
                }
            },
            xaxis: {
                lines: {
                    show: true,
                }
            }
        },
        yaxis: {
            labels: {
                show: false
            }
        },
        title: {
            text: 'Quarterly Results',
            offsetX: 10,
            style: {
                fontWeight: 500,
            },
        },
        tooltip: {
            x: {
                formatter: function (val, opts) {
                    return opts.w.globals.seriesNames[opts.seriesIndex];
                }
            },
            y: {
                title: {
                    formatter: function (val, opts) {
                        return opts.w.globals.labels[opts.dataPointIndex];
                    }
                }
            }
        }
    };

    return (
        <ReactApexChart dir="ltr" className="apex-charts"
            series={series}
            options={optionsQuarter}
            type="bar"
            height={330}
        />
    );
};

const DistributedColumn = ({dataColors}) => {
    var chartColumnDistributedColors = getChartColorsArray(dataColors);
    const series = [{
        data: [21, 22, 10, 28, 16, 21, 13, 30]
    }];
    var options = {
        chart: {
            height: 350,
            type: 'bar',
            events: {
                click: function (chart, w, e) {
                }
            }
        },
        colors: chartColumnDistributedColors,
        plotOptions: {
            bar: {
                columnWidth: '45%',
                distributed: true,
            }
        },
        dataLabels: {
            enabled: false
        },
        legend: {
            show: false
        },
        xaxis: {
            categories: [
                ['John', 'Doe'],
                ['Joe', 'Smith'],
                ['Jake', 'Williams'],
                'Amber',
                ['Peter', 'Brown'],
                ['Mary', 'Evans'],
                ['David', 'Wilson'],
                ['Lily', 'Roberts'],
            ],
            labels: {
                style: {
                    colors: [
                        '#038edc',
                        '#51d28c',
                        '#f7cc53',
                        '#f34e4e',
                        '#564ab1',
                        '#5fd0f3',
                    ],
                    fontSize: '12px'
                }
            }
        }
    };

    return <ReactApexChart dir="ltr" className="apex-charts" series={series} options={options} type="bar" height={350} />;
};


const ColumnGroupLabels = ({ dataColors }) => {
    var chartColumnDistributedColors = getChartColorsArray(dataColors);
    // dayjs.extend(window.dayjs_plugin_quarterOfYear)
    const series = [{
        name: "sales",
        data: [{
            x: '2020/01/01',
            y: 400
        }, {
            x: '2020/04/01',
            y: 430
        }, {
            x: '2020/07/01',
            y: 448
        }, {
            x: '2020/10/01',
            y: 470
        }, {
            x: '2021/01/01',
            y: 540
        }, {
            x: '2021/04/01',
            y: 580
        }, {
            x: '2021/07/01',
            y: 690
        }, {
            x: '2021/10/01',
            y: 690
        }]
    }];
    var options = {
        chart: {
            height: 350,
            type: 'bar',
            toolbar: {
                show: false,
            }
        },
        colors: chartColumnDistributedColors,
        plotOptions: {
            bar: {
                horizontal: false,
                columnWidth: '45%',
            }
        },
        dataLabels: {
            enabled: false
        },
        legend: {
            show: false
        },
        xaxis: {
            type: 'category',
            // labels: {
            //     formatter: function (val) {
            //         return "Q" + dayjs(val).quarter()
            //     }
            // },
            group: {
                style: {
                    fontSize: '10px',
                    fontWeight: 700
                },
                groups: [
                    { title: '2020', cols: 4 },
                    { title: '2021', cols: 4 }
                ]
            }
        },
        title: {
            text: 'Grouped Labels on the X-axis',
            style: {
                fontWeight: 500,
            },
        },
        tooltip: {
            // x: {
            //     formatter: function (val) {
            //         return "Q" + dayjs(val).quarter() + " " + dayjs(val).format("YYYY")
            //     }
            // }
        },
    };

    return <ReactApexChart dir="ltr" className="apex-charts" series={series} options={options} type="bar" height={350} />;
};


export {
    BasicColumn,
    ColumnWithLable,
    StackedColumn,
    StackedColumn2,
    ColumnMarker,
    RotateLable,
    NagetiveLable,
    RangeColumn,
    DynamicColumn,
    Quarter,
    DistributedColumn,
    ColumnGroupLabels
};