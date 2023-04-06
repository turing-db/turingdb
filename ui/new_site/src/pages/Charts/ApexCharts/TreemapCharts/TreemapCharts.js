import React from 'react';
import ReactApexChart from "react-apexcharts";

import getChartColorsArray from "../../../../Components/Common/ChartsDynamicColor";

const BasicTreemap = ({ dataColors }) => {
    var chartTreemapBasicColors = getChartColorsArray(dataColors);
    const series = [{
        data: [{
            x: 'New Delhi',
            y: 218
        },
        {
            x: 'Kolkata',
            y: 149
        },
        {
            x: 'Mumbai',
            y: 184
        },
        {
            x: 'Ahmedabad',
            y: 55
        },
        {
            x: 'Bangaluru',
            y: 84
        },
        {
            x: 'Pune',
            y: 31
        },
        {
            x: 'Chennai',
            y: 70
        },
        {
            x: 'Jaipur',
            y: 30
        },
        {
            x: 'Surat',
            y: 44
        },
        {
            x: 'Hyderabad',
            y: 68
        },
        {
            x: 'Lucknow',
            y: 28
        },
        {
            x: 'Indore',
            y: 19
        },
        {
            x: 'Kanpur',
            y: 29
        }
        ]
    }]
    var options = {

        legend: {
            show: false
        },
        chart: {
            height: 350,
            type: 'treemap',
            toolbar: {
                show: false
            }
        },
        colors: chartTreemapBasicColors,
        title: {
            text: 'Basic Treemap',
            style: {
                fontWeight: 500,
            }
        },
    };
    return (
        <ReactApexChart dir="ltr"
            className="apex-charts"
            series={series}
            options={options}
            type="treemap"
            height={365}
        />
    )

}


const MultiTreemap = ({ dataColors }) => {
    var chartTreemapMultiColors = getChartColorsArray(dataColors);
    const series = [{
        name: 'Desktops',
        data: [{
            x: 'ABC',
            y: 10
        },
        {
            x: 'DEF',
            y: 60
        },
        {
            x: 'XYZ',
            y: 41
        }
        ]
    },
    {
        name: 'Mobile',
        data: [{
            x: 'ABCD',
            y: 10
        },
        {
            x: 'DEFG',
            y: 20
        },
        {
            x: 'WXYZ',
            y: 51
        },
        {
            x: 'PQR',
            y: 30
        },
        {
            x: 'MNO',
            y: 20
        },
        {
            x: 'CDE',
            y: 30
        }
        ]
    }
    ]
    var options = {

        legend: {
            show: false
        },
        chart: {
            height: 350,
            type: 'treemap',
            toolbar: {
                show: false
            }
        },
        title: {
            text: 'Multi-dimensional Treemap',
            align: 'center',
            style: {
                fontWeight: 500,
            }
        },
        colors: chartTreemapMultiColors
    };
    return (
        <ReactApexChart dir="ltr"
            className="apex-charts"
            series={series}
            options={options}
            type="treemap"
            height={365}
        />
    )
}

//Different color
const DiffColorTreemap = ({ dataColors }) => {
    var chartTreemapDiffColor = getChartColorsArray(dataColors);
    const series = [{
        data: [{
            x: 'New Delhi',
            y: 218
        },
        {
            x: 'Kolkata',
            y: 149
        },
        {
            x: 'Mumbai',
            y: 184
        },
        {
            x: 'Ahmedabad',
            y: 55
        },
        {
            x: 'Bangaluru',
            y: 84
        },
        {
            x: 'Pune',
            y: 31
        },
        {
            x: 'Chennai',
            y: 70
        },
        {
            x: 'Jaipur',
            y: 30
        },
        {
            x: 'Surat',
            y: 44
        },
        {
            x: 'Hyderabad',
            y: 68
        },
        {
            x: 'Lucknow',
            y: 28
        },
        {
            x: 'Indore',
            y: 19
        },
        {
            x: 'Kanpur',
            y: 29
        }
        ]
    }]
    var options = {
        legend: {
            show: false
        },
        chart: {
            height: 350,
            type: 'treemap',
            toolbar: {
                show: false
            }
        },
        title: {
            text: 'Distibuted Treemap (different color for each cell)',
            align: 'center',
            style: {
                fontWeight: 500,
            }
        },
        colors: chartTreemapDiffColor,
        plotOptions: {
            treemap: {
                distributed: true,
                enableShades: false
            }
        }
    };
    return (
        <ReactApexChart dir="ltr"
            className="apex-charts"
            series={series}
            options={options}
            type="treemap"
            height={365}
        />
    )
}

//color range
const ColorRangeTreemap = ({ dataColors }) => {
    var chartTreemapRangeColors = getChartColorsArray(dataColors);
    const series = [{
        data: [{
            x: 'INTC',
            y: 1.2
        },
        {
            x: 'GS',
            y: 0.4
        },
        {
            x: 'CVX',
            y: -1.4
        },
        {
            x: 'GE',
            y: 2.7
        },
        {
            x: 'CAT',
            y: -0.3
        },
        {
            x: 'RTX',
            y: 5.1
        },
        {
            x: 'CSCO',
            y: -2.3
        },
        {
            x: 'JNJ',
            y: 2.1
        },
        {
            x: 'PG',
            y: 0.3
        },
        {
            x: 'TRV',
            y: 0.12
        },
        {
            x: 'MMM',
            y: -2.31
        },
        {
            x: 'NKE',
            y: 3.98
        },
        {
            x: 'IYT',
            y: 1.67
        }
        ]
    }]
    var options = {

        legend: {
            show: false
        },
        chart: {
            height: 350,
            type: 'treemap',
            toolbar: {
                show: false
            }
        },
        title: {
            text: 'Treemap with Color scale',
            style: {
                fontWeight: 500,
            }
        },
        dataLabels: {
            enabled: true,
            style: {
                fontSize: '12px',
            },
            formatter: function (text, op) {
                return [text, op.value]
            },
            offsetY: -4
        },
        plotOptions: {
            treemap: {
                enableShades: true,
                shadeIntensity: 0.5,
                reverseNegativeShade: true,
                colorScale: {
                    ranges: [{
                        from: -6,
                        to: 0,
                        color: chartTreemapRangeColors[0]
                    },
                    {
                        from: 0.001,
                        to: 6,
                        color: chartTreemapRangeColors[1]
                    }
                    ]
                }
            }
        }
    };
    return (
        <ReactApexChart dir="ltr"
            className="apex-charts"
            series={series}
            options={options}
            type="treemap"
            height={365}
        />
    )
}

export { BasicTreemap, MultiTreemap, DiffColorTreemap, ColorRangeTreemap }