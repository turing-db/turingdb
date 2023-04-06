import React from 'react';
import ReactApexChart from "react-apexcharts";

import getChartColorsArray from "../../../../Components/Common/ChartsDynamicColor";

const SimpleRadar = ({ dataColors }) => {
    var chartRadarBasicColors = getChartColorsArray(dataColors);
    const series = [{
        name: 'Series 1',
        data: [80, 50, 30, 40, 100, 20],
    }]
    var options = {

        chart: {
            height: 350,
            type: 'radar',
            toolbar: {
                show: false
            }
        },
        colors: chartRadarBasicColors,
        xaxis: {
            categories: ['January', 'February', 'March', 'April', 'May', 'June']
        }
    };
    return (
        <ReactApexChart dir="ltr"
            className="apex-charts"
            series={series}
            options={options}
            type="radar"
            height={365}
        />
    )
}

const MultipleRadar = ({ dataColors }) => {
    var chartRadarMultiColors = getChartColorsArray(dataColors);
    const series = [{
        name: 'Series 1',
        data: [80, 50, 30, 40, 100, 20],
    },
    {
        name: 'Series 2',
        data: [20, 30, 40, 80, 20, 80],
    },
    {
        name: 'Series 3',
        data: [44, 76, 78, 13, 43, 10],
    }
    ]
    var options = {
        chart: {
            height: 350,
            type: 'radar',
            dropShadow: {
                enabled: true,
                blur: 1,
                left: 1,
                top: 1
            },
            toolbar: {
                show: false
            },
        },
        stroke: {
            width: 2
        },
        fill: {
            opacity: 0.2
        },
        markers: {
            size: 0
        },
        colors: chartRadarMultiColors,
        xaxis: {
            categories: ['2014', '2015', '2016', '2017', '2018', '2019']
        }
    };
    return (
        <ReactApexChart dir="ltr"
            className="apex-charts"
            series={series}
            options={options}
            type="radar"
            height={365}
        />
    )
}

const PolygonRadar = ({ dataColors }) => {
    var chartRadarPolyradarColors = getChartColorsArray(dataColors);
    const series = [{
        name: 'Series 1',
        data: [20, 100, 40, 30, 50, 80, 33],
    }]
    var options = {
        chart: {
            height: 350,
            type: 'radar',
            toolbar: {
                show: false
            },
        },
        dataLabels: {
            enabled: true
        },
        plotOptions: {
            radar: {
                size: 140,

            }
        },
        title: {
            text: 'Radar with Polygon Fill',
            style: {
                fontWeight: 500,
            },
        },
        colors: chartRadarPolyradarColors,
        markers: {
            size: 4,
            colors: ['#fff'],
            strokeColor: '#f34e4e',
            strokeWidth: 2,
        },
        tooltip: {
            y: {
                formatter: function (val) {
                    return val
                }
            }
        },
        xaxis: {
            categories: ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']
        },
        yaxis: {
            tickAmount: 7,
            labels: {
                formatter: function (val, i) {
                    if (i % 2 === 0) {
                        return val
                    } else {
                        return ''
                    }
                }
            }
        }
    };
    return (
        <ReactApexChart dir="ltr"
            className="apex-charts"
            series={series}
            options={options}
            type="radar"
            height={365}
        />
    )
}

export { SimpleRadar, MultipleRadar, PolygonRadar }