import React from "react";
import ReactApexChart from "react-apexcharts";
import getChartColorsArray from "../../Components/Common/ChartsDynamicColor";

const MarketplaceChart = ({ dataColors, series }) => {
    var MarketplaceChartColors = getChartColorsArray(dataColors);

    var options = {
        chart: {
            height: 350,
            type: 'line',
            zoom: {
                enabled: false
            },
            toolbar: {
                show: false
            }
        },
        // markers: {
        //     size: 4,
        // },
        dataLabels: {
            enabled: false
        },
        stroke: {
            curve: 'smooth',
            width: 3
        },
        colors: MarketplaceChartColors,
        xaxis: {
            categories: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep'],
        }
    };
    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                options={options}
                series={series}
                type="line"
                height="350"
                className="apex-charts"
            />
        </React.Fragment>
    );
};

const PopularityChart = ({ dataColors }) => {
    var PopularityChartColors = getChartColorsArray(dataColors);
    const series = [{
        name: 'Like',
        data: [12.45, 16.2, 8.9, 11.42, 12.6, 18.1, 18.2, 14.16]
    }, {
        name: 'Share',
        data: [-11.45, -15.42, -7.9, -12.42, -12.6, -18.1, -18.2, -14.16]
    }];

    const options = {
        chart: {
            type: 'bar',
            height: 260,
            stacked: true,
            toolbar: {
                show: false
            },
        },
        plotOptions: {
            bar: {
                columnWidth: '20%',
                borderRadius: [4, 4]
            },
        },
        dataLabels: {
            enabled: false,
            textAnchor: 'top',
        },
        colors: PopularityChartColors,

        fill: {
            opacity: 1
        },
        legend: {
            position: 'top',
            horizontalAlign: 'right',
        },
        yaxis: {
            labels: {
                show: false,
                formatter: function (y) {
                    return y.toFixed(0) + "%";
                }
            }
        },
        xaxis: {
            categories: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug'],
            labels: {
                rotate: -90
            }
        }
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                options={options}
                series={series}
                type="bar"
                height={260}
                className="apex-charts mt-n4"
            />
        </React.Fragment>
    );
};

const TopArtworkChart = ({ seriesData, chartsColor }) => {
    const topartworkColors = [chartsColor];
    var options = {
        chart: {
            width: 80,
            height: 30,
            type: "line",
            sparkline: {
                enabled: true,
            },
            toolbar: {
                show: false,
            },
        },
        stroke: {
            curve: "smooth",
            width: 2.3,
        },
        tooltip: {
            fixed: {
                enabled: false
            },
            x: {
                show: false
            },
            y: {
                title: {
                    formatter: function (seriesName) {
                        return '';
                    }
                }
            },
            marker: {
                show: false
            }
        },
        colors: topartworkColors
    };
    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                options={options}
                series={[...seriesData]}
                type="line"
                height="30"
                width="80"
                className="apex-charts"
            />
        </React.Fragment>
    );
};

export { MarketplaceChart, PopularityChart, TopArtworkChart };