import React from 'react';
import ReactApexChart from "react-apexcharts";

import getChartColorsArray from "../../Components/Common/ChartsDynamicColor";

const SimpleDonutCharts = ({ dataColors }) => {
    var chartDonutBasicColors = getChartColorsArray(dataColors);
    const series = [27.01, 20.87, 33.54, 37.58]
    var options = {
        chart: {
            height: 330,
            type: 'donut',
        },
        labels: ["Documents", "Media", "Others", "Free Space"],
        dataLabels: {
            dropShadow: {
                enabled: false,
              }
        },
        legend: {
            position: 'bottom'
        },
        colors: chartDonutBasicColors
    };
    return (
        <ReactApexChart dir="ltr"
            series={series}
            options={options}
            type="donut"
            height={330}
            className="apex-charts mt-3"
        />

    )
}

export default SimpleDonutCharts;
