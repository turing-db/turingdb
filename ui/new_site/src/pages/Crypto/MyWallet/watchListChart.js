import React from 'react';
import ReactApexChart from "react-apexcharts";

const WatchListChart = ({ seriesData, chartsColor }) => {
    const areachartlitecoinColors = [chartsColor];
    var options = {
        chart: {
            width: 130,
            height: 46,
            type: "area",
            sparkline: {
                enabled: true,
            },
            toolbar: {
                show: false,
            },
        },
        dataLabels: {
            enabled: false,
        },
        stroke: {
            curve: "smooth",
            width: 1.5,
        },
        fill: {
            type: "gradient",
            gradient: {
                shadeIntensity: 1,
                inverseColors: false,
                opacityFrom: 0.45,
                opacityTo: 0.05,
                stops: [50, 100, 100, 100],
            },
        },
        colors: areachartlitecoinColors
    };
    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                options={options}
                series={[...seriesData]}
                type="area"
                height="46"
                className="apex-charts"
            />
        </React.Fragment>
    );
};

export default WatchListChart;