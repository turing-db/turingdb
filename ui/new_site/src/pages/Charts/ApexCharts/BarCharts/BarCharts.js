import React from 'react';
import ReactApexChart from "react-apexcharts";
import user from "../../../../assets/images/small/img-4.jpg";

import getChartColorsArray from "../../../../Components/Common/ChartsDynamicColor";

const Basic = ({ dataColors }) => {
    var BasicColors = getChartColorsArray(dataColors);
    const series = [{
        data: [380, 430, 450, 475, 550, 584, 780, 1100, 1220, 1365]
    }];

    const options = {
        chart: {
            toolbar: {
                show: !1,
            }
        },
        plotOptions: {
            bar: {
                horizontal: !0,
            }
        },
        dataLabels: {
            enabled: !1
        },
        colors: BasicColors,
        grid: {
            borderColor: "#f1f1f1",
        },
        xaxis: {
            categories: ["South Korea", "Canada", "United Kingdom", "Netherlands", "Italy", "France", "Japan", "United States", "China", "Germany"],
        }
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={350}
            />
        </React.Fragment>
    );
};

const CustomDataLabel = ({ dataColors }) => {
    var chartDatalabelsBarColors = getChartColorsArray(dataColors);
    const series = [{
        data: [400, 430, 448, 470, 540, 580, 690, 1100, 1200, 1380]
    }];
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
                barHeight: '100%',
                distributed: true,
                horizontal: true,
                dataLabels: {
                    position: 'bottom'
                },
            }
        },
        colors: chartDatalabelsBarColors,
        dataLabels: {
            enabled: true,
            textAnchor: 'start',
            style: {
                colors: ['#fff']
            },
            formatter: function (val, opt) {
                return opt.w.globals.labels[opt.dataPointIndex] + ":  " + val;
            },
            offsetX: 0,
            dropShadow: {
                enabled: false
            }
        },
        stroke: {
            width: 1,
            colors: ['#fff']
        },
        xaxis: {
            categories: ['South Korea', 'Canada', 'United Kingdom', 'Netherlands', 'Italy', 'France', 'Japan',
                'United States', 'China', 'India'
            ],
        },
        yaxis: {
            labels: {
                show: false
            }
        },
        title: {
            text: 'Custom DataLabels',
            align: 'center',
            floating: true,
            style: {
                fontWeight: 500,
            },
        },
        subtitle: {
            text: 'Category Names as DataLabels inside bars',
            align: 'center',
        },
        tooltip: {
            theme: 'dark',
            x: {
                show: false
            },
            y: {
                title: {
                    formatter: function () {
                        return '';
                    }
                }
            }
        }
    };
    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={350}
            />
        </React.Fragment>
    );
};

const Stacked = ({ dataColors }) => {
    var chartStackedBarColors = getChartColorsArray(dataColors);
    const series = [{
        name: 'Marine Sprite',
        data: [44, 55, 41, 37, 22, 43, 21]
    }, {
        name: 'Striking Calf',
        data: [53, 32, 33, 52, 13, 43, 32]
    }, {
        name: 'Tank Picture',
        data: [12, 17, 11, 9, 15, 11, 20]
    }, {
        name: 'Bucket Slope',
        data: [9, 7, 5, 8, 6, 9, 4]
    }, {
        name: 'Reborn Kid',
        data: [25, 12, 19, 32, 25, 24, 10]
    }];
    var options = {
        chart: {
            type: 'bar',
            height: 350,
            stacked: true,
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                horizontal: true,
            },
        },
        stroke: {
            width: 1,
            colors: ['#fff']
        },
        title: {
            text: 'Fiction Books Sales',
            style: {
                fontWeight: 500,
            },
        },
        xaxis: {
            categories: [2008, 2009, 2010, 2011, 2012, 2013, 2014],
            labels: {
                formatter: function (val) {
                    return val + "K";
                }
            }
        },
        yaxis: {
            title: {
                text: undefined
            },
        },
        tooltip: {
            y: {
                formatter: function (val) {
                    return val + "K";
                }
            }
        },
        fill: {
            opacity: 1
        },
        legend: {
            position: 'top',
            horizontalAlign: 'left',
            offsetX: 40
        },
        colors: chartStackedBarColors,
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={350}
            />
        </React.Fragment>
    );
};

const Stacked2 = ({ dataColors }) => {
    var chartStackedBar100Colors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Marine Sprite",
            data: [44, 55, 41, 37, 22, 43, 21],
        },
        {
            name: "Striking Calf",
            data: [53, 32, 33, 52, 13, 43, 32],
        },
        {
            name: "Tank Picture",
            data: [12, 17, 11, 9, 15, 11, 20],
        },
        {
            name: "Bucket Slope",
            data: [9, 7, 5, 8, 6, 9, 4],
        },
        {
            name: "Reborn Kid",
            data: [25, 12, 19, 32, 25, 24, 10],
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
        plotOptions: {
            bar: {
                horizontal: !0,
            },
        },
        stroke: {
            width: 1,
            colors: ["#fff"],
        },
        title: {
            text: "100% Stacked Bar",
            style: {
                fontWeight: 500,
            },
        },
        xaxis: {
            categories: [2008, 2009, 2010, 2011, 2012, 2013, 2014],
        },
        tooltip: {
            y: {
                formatter: function (val) {
                    return val + "K";
                },
            },
        },
        fill: {
            opacity: 1,
        },
        legend: {
            position: "top",
            horizontalAlign: "left",
            offsetX: 40,
        },
        colors: chartStackedBar100Colors,
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={350}
            />
        </React.Fragment>
    );
};

const Negative = ({ dataColors }) => {
    var chartNegativeBarColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Males",
            data: [
                0.4, 0.65, 0.76, 0.88, 1.5, 2.1, 2.9, 3.8, 3.9, 4.2, 4, 4.3, 4.1, 4.2,
                4.5, 3.9, 3.5, 3,
            ],
        },
        {
            name: "Females",
            data: [
                -0.8, -1.05, -1.06, -1.18, -1.4, -2.2, -2.85, -3.7, -3.96, -4.22, -4.3,
                -4.4, -4.1, -4, -4.1, -3.4, -3.1, -2.8,
            ],
        },
    ];

    const options = {
        chart: {
            type: "bar",
            height: 360,
            stacked: !0,
            toolbar: {
                show: !1,
            },
        },
        colors: chartNegativeBarColors,
        plotOptions: {
            bar: {
                horizontal: !0,
                barHeight: "80%",
            },
        },
        dataLabels: {
            enabled: !1,
        },
        stroke: {
            width: 1,
            colors: ["#fff"],
        },

        grid: {
            xaxis: {
                lines: {
                    show: !1,
                },
            },
        },
        yaxis: {
            min: -5,
            max: 5,
            title: {
                text: "Age",
                style: {
                    fontWeight: 600,
                },
            },
        },
        tooltip: {
            shared: !1,
            x: {
                formatter: function (val) {
                    return val;
                },
            },
            y: {
                formatter: function (val) {
                    return Math.abs(val) + "%";
                },
            },
        },
        title: {
            text: "Mauritius population pyramid 2011",
            style: {
                fontWeight: 600,
            },
        },
        xaxis: {
            categories: [
                "85+",
                "80-84",
                "75-79",
                "70-74",
                "65-69",
                "60-64",
                "55-59",
                "50-54",
                "45-49",
                "40-44",
                "35-39",
                "30-34",
                "25-29",
                "20-24",
                "15-19",
                "10-14",
                "5-9",
                "0-4",
            ],
            title: {
                text: "Percent",
            },
            labels: {
                formatter: function (val) {
                    return Math.abs(Math.round(val)) + "%";
                },
            },
        },
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={350}
            />
        </React.Fragment>
    );
};

const Markers = ({ dataColors }) => {
    var chartBarMarkersColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Actual",
            data: [
                {
                    x: "2011",
                    y: 12,
                    goals: [
                        {
                            name: "Expected",
                            value: 14,
                            strokeWidth: 5,
                            strokeColor: "#564ab1",
                        },
                    ],
                },
                {
                    x: "2012",
                    y: 44,
                    goals: [
                        {
                            name: "Expected",
                            value: 54,
                            strokeWidth: 5,
                            strokeColor: "#564ab1",
                        },
                    ],
                },
                {
                    x: "2013",
                    y: 54,
                    goals: [
                        {
                            name: "Expected",
                            value: 52,
                            strokeWidth: 5,
                            strokeColor: "#564ab1",
                        },
                    ],
                },
                {
                    x: "2014",
                    y: 66,
                    goals: [
                        {
                            name: "Expected",
                            value: 65,
                            strokeWidth: 5,
                            strokeColor: "#564ab1",
                        },
                    ],
                },
                {
                    x: "2015",
                    y: 81,
                    goals: [
                        {
                            name: "Expected",
                            value: 66,
                            strokeWidth: 5,
                            strokeColor: "#564ab1",
                        },
                    ],
                },
                {
                    x: "2016",
                    y: 67,
                    goals: [
                        {
                            name: "Expected",
                            value: 70,
                            strokeWidth: 5,
                            strokeColor: "#564ab1",
                        },
                    ],
                },
            ],
        },
    ];

    var options = {
        chart: {
            toolbar: {
                show: !1,
            },
        },
        plotOptions: {
            bar: {
                horizontal: !0,
            },
        },
        colors: chartBarMarkersColors,
        dataLabels: {
            formatter: function (val, opt) {
                var goals =
                    opt.w.config.series[opt.seriesIndex].data[opt.dataPointIndex].goals;

                if (goals && goals.length) {
                    return `${val} / ${goals[0].value}`
                }
                return val;
            },
        },
        legend: {
            show: !0,
            showForSingleSeries: !0,
            customLegendItems: ["Actual", "Expected"],
            Markers: {
                fillColors: ["#00E396", "#775DD0"],
            },
        },
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={350}
            />
        </React.Fragment>
    );
};

const Reversed = ({ dataColors }) => {
    var chartBarReversedColors = getChartColorsArray(dataColors);
    const series = [
        {
            data: [400, 430, 448, 470, 540, 580, 690],
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
        annotations: {
            xaxis: [{
                x: 500,
                borderColor: '#038edc',
                label: {
                    borderColor: '#038edc',
                    style: {
                        color: '#fff',
                        background: '#038edc',
                    },
                    text: 'X annotation',
                }
            }],
            yaxis: [{
                y: 'July',
                y2: 'September',
                label: {
                    text: 'Y annotation'
                }
            }]
        },
        colors: chartBarReversedColors,
        plotOptions: {
            bar: {
                horizontal: true,
            }
        },
        dataLabels: {
            enabled: true
        },
        xaxis: {
            categories: ['June', 'July', 'August', 'September', 'October', 'November', 'December'],
        },
        grid: {
            xaxis: {
                lines: {
                    show: true
                }
            }
        },
        yaxis: {
            reversed: true,
            axisTicks: {
                show: true
            }
        }
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={350}
            />
        </React.Fragment>
    );
};

const Patterned = ({ dataColors }) => {
    var chartPatternedColors = getChartColorsArray(dataColors);
    const series = [{
        name: 'Marine Sprite',
        data: [44, 55, 41, 37, 22, 43, 21]
    }, {
        name: 'Striking Calf',
        data: [53, 32, 33, 52, 13, 43, 32]
    }, {
        name: 'Tank Picture',
        data: [12, 17, 11, 9, 15, 11, 20]
    }, {
        name: 'Bucket Slope',
        data: [9, 7, 5, 8, 6, 9, 4]
    }];
    var options = {
        chart: {
            type: 'bar',
            height: 350,
            stacked: true,
            dropShadow: {
                enabled: true,
                blur: 1,
                opacity: 0.25
            },
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                horizontal: true,
                barHeight: '60%',
            },
        },
        dataLabels: {
            enabled: false
        },
        stroke: {
            width: 2,
        },
        title: {
            text: 'Compare Sales Strategy',
            style: {
                fontWeight: 500,
            },
        },
        xaxis: {
            categories: [2008, 2009, 2010, 2011, 2012, 2013, 2014],
        },
        yaxis: {
            title: {
                text: undefined
            },
        },
        tooltip: {
            shared: false,
            y: {
                formatter: function (val) {
                    return val + "K";
                }
            }
        },
        fill: {
            type: 'pattern',
            opacity: 1,
            pattern: {
                style: ['circles', 'slantedLines', 'verticalLines', 'horizontalLines'], // string or array of strings

            }
        },
        states: {
            hover: {
                filter: 'none'
            }
        },
        legend: {
            position: 'right',
            offsetY: 40
        },
        colors: chartPatternedColors
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={350}
            />
        </React.Fragment>
    );
};

const Groupes = ({ dataColors }) => {
    var chartGroupbarColors = getChartColorsArray(dataColors);
    const series = [
        {
            data: [44, 55, 41, 64, 22, 43, 21],
        },
        {
            data: [53, 32, 33, 52, 13, 44, 32],
        },
    ];

    var options = {
        chart: {
            type: 'bar',
            height: 410,
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                horizontal: true,
                dataLabels: {
                    position: 'top',
                },
            }
        },
        dataLabels: {
            enabled: true,
            offsetX: -6,
            style: {
                fontSize: '12px',
                colors: ['#fff']
            }
        },
        stroke: {
            show: true,
            width: 1,
            colors: ['#fff']
        },
        tooltip: {
            shared: true,
            intersect: false
        },
        xaxis: {
            categories: [2001, 2002, 2003, 2004, 2005, 2006, 2007],
        },
        colors: chartGroupbarColors
    };
    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={410}
            />
        </React.Fragment>
    );
};

const BarwithImages = () => {
    const series = [{
        name: 'coins',
        data: [2, 4, 3, 4, 3, 5, 5, 6.5, 6, 5, 4, 5, 8, 7, 7, 8, 8, 10, 9, 9, 12, 12,
            11, 12, 13, 14, 16, 14, 15, 17, 19, 21
        ]
    }];

    var options = {
        chart: {
            type: 'bar',
            height: 410,
            animations: {
                enabled: false
            },
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                horizontal: true,
                barHeight: '100%',

            },
        },
        dataLabels: {
            enabled: false,
        },
        stroke: {
            colors: ["#fff"],
            width: 0.2
        },
        labels: Array.apply(null, {
            length: 39
        }).map(function (el, index) {
            return index + 1;
        }),
        yaxis: {
            axisBorder: {
                show: false
            },
            axisTicks: {
                show: false
            },
            labels: {
                show: false
            },
            title: {
                text: 'Weight',
            },
        },
        grid: {
            position: 'back'
        },
        title: {
            text: 'Paths filled by clipped image',
            align: 'right',
            offsetY: 30,
            style: {
                fontWeight: 500,
            },
        },
        fill: {
            type: 'image',
            opacity: 0.87,
            image: {
                src: [user],
                width: 466,
                height: 406
            }
        },
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="bar"
                height={410}
            />
        </React.Fragment>
    );
};


export {
    Basic,
    CustomDataLabel,
    Stacked,
    Stacked2,
    Negative,
    Markers,
    Reversed,
    Patterned,
    Groupes,
    BarwithImages
};