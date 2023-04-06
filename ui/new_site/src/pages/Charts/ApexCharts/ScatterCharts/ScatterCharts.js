import React from "react";
import ReactApexChart from "react-apexcharts";
import avatar1 from "../../../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../../../assets/images/users/avatar-2.jpg";

import getChartColorsArray from "../../../../Components/Common/ChartsDynamicColor";

const Basic = ({ dataColors }) => {
    var chartScatterBasicColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "SAMPLE A",
            data: [
                [16.4, 5.4],
                [21.7, 2],
                [25.4, 3],
                [19, 2],
                [10.9, 1],
                [13.6, 3.2],
                [10.9, 7.4],
                [10.9, 0],
                [10.9, 8.2],
                [16.4, 0],
                [16.4, 1.8],
                [13.6, 0.3],
                [13.6, 0],
                [29.9, 0],
                [27.1, 2.3],
                [16.4, 0],
                [13.6, 3.7],
                [10.9, 5.2],
                [16.4, 6.5],
                [10.9, 0],
                [24.5, 7.1],
                [10.9, 0],
                [8.1, 4.7],
                [19, 0],
                [21.7, 1.8],
                [27.1, 0],
                [24.5, 0],
                [27.1, 0],
                [29.9, 1.5],
                [27.1, 0.8],
                [22.1, 2],
            ],
        },
        {
            name: "SAMPLE B",
            data: [
                [36.4, 13.4],
                [1.7, 11],
                [5.4, 8],
                [9, 17],
                [1.9, 4],
                [3.6, 12.2],
                [1.9, 14.4],
                [1.9, 9],
                [1.9, 13.2],
                [1.4, 7],
                [6.4, 8.8],
                [3.6, 4.3],
                [1.6, 10],
                [9.9, 2],
                [7.1, 15],
                [1.4, 0],
                [3.6, 13.7],
                [1.9, 15.2],
                [6.4, 16.5],
                [0.9, 10],
                [4.5, 17.1],
                [10.9, 10],
                [0.1, 14.7],
                [9, 10],
                [12.7, 11.8],
                [2.1, 10],
                [2.5, 10],
                [27.1, 10],
                [2.9, 11.5],
                [7.1, 10.8],
                [2.1, 12],
            ],
        },
        {
            name: "SAMPLE C",
            data: [
                [21.7, 3],
                [23.6, 3.5],
                [24.6, 3],
                [29.9, 3],
                [21.7, 20],
                [23, 2],
                [10.9, 3],
                [28, 4],
                [27.1, 0.3],
                [16.4, 4],
                [13.6, 0],
                [19, 5],
                [22.4, 3],
                [24.5, 3],
                [32.6, 3],
                [27.1, 4],
                [29.6, 6],
                [31.6, 8],
                [21.6, 5],
                [20.9, 4],
                [22.4, 0],
                [32.6, 10.3],
                [29.7, 20.8],
                [24.5, 0.8],
                [21.4, 0],
                [21.7, 6.9],
                [28.6, 7.7],
                [15.4, 0],
                [18.1, 0],
                [33.4, 0],
                [16.4, 0],
            ],
        },
    ];
    const options = {
        chart: {
            zoom: {
                enabled: true,
                type: "xy",
            },
            toolbar: {
                show: false,
            },
        },
        xaxis: {
            tickAmount: 10,
            labels: {
                formatter: function (val) {
                    return parseFloat(val).toFixed(1);
                },
            },
        },
        yaxis: {
            tickAmount: 7,
        },
        colors: chartScatterBasicColors,
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="scatter"
                height={350}
            />
        </React.Fragment>
    );
};

const Datetime = ({ dataColors }) => {
    var chartScatterDateTimeColors = getChartColorsArray(dataColors);
    const generateDayWiseTimeSeries = (baseval, count, yrange) => {
        var i = 0;
        var series = [];
        while (i < count) {
            var y =
                Math.floor(Math.random() * (yrange.max - yrange.min + 1)) + yrange.min;

            series.push([baseval, y]);
            baseval += 86400000;
            i++;
        }
        return series;
    };

    const series = [
        {
            name: "TEAM 1",
            data: generateDayWiseTimeSeries(
                new Date("11 Feb 2017 GMT").getTime(),
                20,
                {
                    min: 10,
                    max: 60,
                }
            ),
        },
        {
            name: "TEAM 2",
            data: generateDayWiseTimeSeries(
                new Date("11 Feb 2017 GMT").getTime(),
                20,
                {
                    min: 10,
                    max: 60,
                }
            ),
        },
        {
            name: "TEAM 3",
            data: generateDayWiseTimeSeries(
                new Date("11 Feb 2017 GMT").getTime(),
                30,
                {
                    min: 10,
                    max: 60,
                }
            ),
        },
        {
            name: "TEAM 4",
            data: generateDayWiseTimeSeries(
                new Date("11 Feb 2017 GMT").getTime(),
                10,
                {
                    min: 10,
                    max: 60,
                }
            ),
        },
        {
            name: "TEAM 5",
            data: generateDayWiseTimeSeries(
                new Date("11 Feb 2017 GMT").getTime(),
                30,
                {
                    min: 10,
                    max: 60,
                }
            ),
        },
    ];

    const options = {
        chart: {
            zoom: {
                type: "xy",
            },
            toolbar: {
                show: !1,
            },
        },
        dataLabels: {
            enabled: !1,
        },
        grid: {
            xaxis: {
                lines: {
                    show: !0,
                },
            },
            yaxis: {
                lines: {
                    show: !0,
                },
            },
        },
        xaxis: {
            type: "datetime",
        },
        yaxis: {
            max: 70,
        },
        colors: chartScatterDateTimeColors,
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="scatter"
                height={350}
            />
        </React.Fragment>
    );
};

const ImagesChart = ({ dataColors }) => {
    var chartScatterImagesColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "User A",
            data: [
                [16.4, 5.4],
                [21.7, 4],
                [25.4, 3],
                [19, 2],
                [10.9, 1],
                [13.6, 3.2],
                [10.9, 7],
                [10.9, 8.2],
                [16.4, 4],
                [13.6, 4.3],
                [13.6, 12],
                [29.9, 3],
                [10.9, 5.2],
                [16.4, 6.5],
                [10.9, 8],
                [24.5, 7.1],
                [10.9, 7],
                [8.1, 4.7],
            ],
        },
        {
            name: "User B",
            data: [
                [6.4, 5.4],
                [11.7, 4],
                [15.4, 3],
                [9, 2],
                [10.9, 11],
                [20.9, 7],
                [12.9, 8.2],
                [6.4, 14],
                [11.6, 12],
            ],
        },
    ];

    const options = {
        chart: {
            animations: {
                enabled: !1,
            },
            zoom: {
                enabled: !1,
            },
            toolbar: {
                show: !1,
            },
        },
        colors: chartScatterImagesColors,
        xaxis: {
            tickAmount: 10,
            min: 0,
            max: 40,
        },
        yaxis: {
            tickAmount: 7,
        },
        markers: {
            size: 20,
        },
        fill: {
            type: "image",
            opacity: 1,
            image: {
                src: [
                    avatar1,
                    avatar2,
                ],
                width: 40,
                height: 40,
            },
        },
        legend: {
            labels: {
                useSeriesColors: !0,
            },
            markers: {
                customHTML: [
                    function () {
                        return "";
                    },
                    function () {
                        return "";
                    },
                ],
            },
        },
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="scatter"
                height={350}
            />
        </React.Fragment>
    );
};

export {
    Basic,
    Datetime,
    ImagesChart
};
