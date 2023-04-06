import React from 'react';
import ReactApexChart from "react-apexcharts";
import * as moment from "moment";

import getChartColorsArray from "../../../../Components/Common/ChartsDynamicColor";

const Basic = ({ dataColors }) => {
    var chartTimelineBasicColors = getChartColorsArray(dataColors);
    const series = [
        {
            data: [
                {
                    x: "Code",
                    y: [
                        new Date("2019-03-02").getTime(),
                        new Date("2019-03-04").getTime(),
                    ],
                },
                {
                    x: "Test",
                    y: [
                        new Date("2019-03-04").getTime(),
                        new Date("2019-03-08").getTime(),
                    ],
                },
                {
                    x: "Validation",
                    y: [
                        new Date("2019-03-08").getTime(),
                        new Date("2019-03-12").getTime(),
                    ],
                },
                {
                    x: "Deployment",
                    y: [
                        new Date("2019-03-12").getTime(),
                        new Date("2019-03-18").getTime(),
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
                horizontal: !0,
            },
        },
        xaxis: {
            type: "datetime",
        },
        colors: chartTimelineBasicColors,
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="rangeBar"
                height={350}
            />
        </React.Fragment>
    );
};

const DifferentColor = ({ dataColors }) => {
    var chartTimelineColors = getChartColorsArray(dataColors);
    const series = [
        {
            data: [
                {
                    x: 'Analysis',
                    y: [
                        new Date('2019-02-27').getTime(),
                        new Date('2019-03-04').getTime()
                    ],
                    fillColor: chartTimelineColors[0]
                },
                {
                    x: 'Design',
                    y: [
                        new Date('2019-03-04').getTime(),
                        new Date('2019-03-08').getTime()
                    ],
                    fillColor: chartTimelineColors[1]
                },
                {
                    x: 'Coding',
                    y: [
                        new Date('2019-03-07').getTime(),
                        new Date('2019-03-10').getTime()
                    ],
                    fillColor: chartTimelineColors[2]
                },
                {
                    x: 'Testing',
                    y: [
                        new Date('2019-03-08').getTime(),
                        new Date('2019-03-12').getTime()
                    ],
                    fillColor: chartTimelineColors[3]
                },
                {
                    x: 'Deployment',
                    y: [
                        new Date('2019-03-12').getTime(),
                        new Date('2019-03-17').getTime()
                    ],
                    fillColor: chartTimelineColors[4]
                },
            ],
        },
    ];

    const options = {
        chart: {
            height: 350,
            type: 'rangeBar',
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                horizontal: true,
                distributed: true,
                dataLabels: {
                    hideOverflowingLabels: false
                }
            }
        },
        dataLabels: {
            enabled: true,
            formatter: function (val, opts) {
                var label = opts.w.globals.labels[opts.dataPointIndex]
                var a = moment(val[0])
                var b = moment(val[1])
                var diff = b.diff(a, 'days')
                return label + ': ' + diff + (diff > 1 ? ' days' : ' day')
            },
        },
        xaxis: {
            type: 'datetime'
        },
        yaxis: {
            show: true
        },
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="rangeBar"
                height={350}
            />
        </React.Fragment>
    );
};

const MultiSeries = ({ dataColors }) => {
    var chartTimelineMultiSeriesColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Bob",
            data: [
                {
                    x: "Design",
                    y: [
                        new Date("2019-03-05").getTime(),
                        new Date("2019-03-08").getTime(),
                    ],
                },
                {
                    x: "Code",
                    y: [
                        new Date("2019-03-08").getTime(),
                        new Date("2019-03-11").getTime(),
                    ],
                },
                {
                    x: "Test",
                    y: [
                        new Date("2019-03-11").getTime(),
                        new Date("2019-03-16").getTime(),
                    ],
                },
            ],
        },
        {
            name: "Joe",
            data: [
                {
                    x: "Design",
                    y: [
                        new Date("2019-03-02").getTime(),
                        new Date("2019-03-05").getTime(),
                    ],
                },
                {
                    x: "Code",
                    y: [
                        new Date("2019-03-06").getTime(),
                        new Date("2019-03-09").getTime(),
                    ],
                },
                {
                    x: "Test",
                    y: [
                        new Date("2019-03-10").getTime(),
                        new Date("2019-03-19").getTime(),
                    ],
                },
            ],
        },
    ];

    var options = {
        chart: {
            toolbar: {
                show: false,
            },
        },
        plotOptions: {
            bar: {
                horizontal: true,
            },
        },
        dataLabels: {
            enabled: true,
            formatter: function (val) {
                var a = moment(val[0]);
                var b = moment(val[1]);
                var diff = b.diff(a, "days");
                return diff + (diff > 1 ? " days" : " day");
            },
        },
        fill: {
            type: "gradient",
            gradient: {
                shade: "light",
                type: "vertical",
                shadeIntensity: 0.25,
                gradientToColors: undefined,
                inverseColors: true,
                opacityFrom: 1,
                opacityTo: 1,
                stops: [50, 0, 100, 100],
            },
        },
        xaxis: {
            type: "datetime",
        },
        legend: {
            position: "top",
        },
        colors: chartTimelineMultiSeriesColors,
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="rangeBar"
                height={350}
            />
        </React.Fragment>
    );
};

const Advanced = ({ dataColors }) => {
    var chartTimelineAdvancedColors = getChartColorsArray(dataColors);
    const series = [
        {
            name: "Bob",
            data: [
                {
                    x: "Design",
                    y: [
                        new Date("2019-03-05").getTime(),
                        new Date("2019-03-08").getTime(),
                    ],
                },
                {
                    x: "Code",
                    y: [
                        new Date("2019-03-02").getTime(),
                        new Date("2019-03-05").getTime(),
                    ],
                },
                {
                    x: "Code",
                    y: [
                        new Date("2019-03-05").getTime(),
                        new Date("2019-03-07").getTime(),
                    ],
                },
                {
                    x: "Test",
                    y: [
                        new Date("2019-03-03").getTime(),
                        new Date("2019-03-09").getTime(),
                    ],
                },
                {
                    x: "Test",
                    y: [
                        new Date("2019-03-08").getTime(),
                        new Date("2019-03-11").getTime(),
                    ],
                },
                {
                    x: "Validation",
                    y: [
                        new Date("2019-03-11").getTime(),
                        new Date("2019-03-16").getTime(),
                    ],
                },
                {
                    x: "Design",
                    y: [
                        new Date("2019-03-01").getTime(),
                        new Date("2019-03-03").getTime(),
                    ],
                },
            ],
        },
        {
            name: "Joe",
            data: [
                {
                    x: "Design",
                    y: [
                        new Date("2019-03-02").getTime(),
                        new Date("2019-03-05").getTime(),
                    ],
                },
                {
                    x: "Test",
                    y: [
                        new Date("2019-03-06").getTime(),
                        new Date("2019-03-16").getTime(),
                    ],
                },
                {
                    x: "Code",
                    y: [
                        new Date("2019-03-03").getTime(),
                        new Date("2019-03-07").getTime(),
                    ],
                },
                {
                    x: "Deployment",
                    y: [
                        new Date("2019-03-20").getTime(),
                        new Date("2019-03-22").getTime(),
                    ],
                },
                {
                    x: "Design",
                    y: [
                        new Date("2019-03-10").getTime(),
                        new Date("2019-03-16").getTime(),
                    ],
                },
            ],
        },
        {
            name: "Dan",
            data: [
                {
                    x: "Code",
                    y: [
                        new Date("2019-03-10").getTime(),
                        new Date("2019-03-17").getTime(),
                    ],
                },
                {
                    x: "Validation",
                    y: [
                        new Date("2019-03-05").getTime(),
                        new Date("2019-03-09").getTime(),
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
                horizontal: !0,
                barHeight: "80%",
            },
        },
        xaxis: {
            type: "datetime",
        },
        stroke: {
            width: 1,
        },
        fill: {
            type: "solid",
            opacity: 0.6,
        },
        legend: {
            position: "top",
            horizontalAlign: "left",
        },
        colors: chartTimelineAdvancedColors,
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="rangeBar"
                height={350}
            />
        </React.Fragment>
    );
};

// Multiple series � Group rows

const MultipleSeries = ({ dataColors }) => {
    var chartMultiSeriesColors = getChartColorsArray(dataColors);
    const series = [
        // George Washington
        {
            name: 'George Washington',
            data: [
                {
                    x: 'President',
                    y: [
                        new Date(1789, 3, 30).getTime(),
                        new Date(1797, 2, 4).getTime()
                    ]
                },
            ]
        },
        // John Adams
        {
            name: 'John Adams',
            data: [
                {
                    x: 'President',
                    y: [
                        new Date(1797, 2, 4).getTime(),
                        new Date(1801, 2, 4).getTime()
                    ]
                },
                {
                    x: 'Vice President',
                    y: [
                        new Date(1789, 3, 21).getTime(),
                        new Date(1797, 2, 4).getTime()
                    ]
                }
            ]
        },
        // Thomas Jefferson
        {
            name: 'Thomas Jefferson',
            data: [
                {
                    x: 'President',
                    y: [
                        new Date(1801, 2, 4).getTime(),
                        new Date(1809, 2, 4).getTime()
                    ]
                },
                {
                    x: 'Vice President',
                    y: [
                        new Date(1797, 2, 4).getTime(),
                        new Date(1801, 2, 4).getTime()
                    ]
                },
                {
                    x: 'Secretary of State',
                    y: [
                        new Date(1790, 2, 22).getTime(),
                        new Date(1793, 11, 31).getTime()
                    ]
                }
            ]
        },
        // Aaron Burr
        {
            name: 'Aaron Burr',
            data: [
                {
                    x: 'Vice President',
                    y: [
                        new Date(1801, 2, 4).getTime(),
                        new Date(1805, 2, 4).getTime()
                    ]
                }
            ]
        },
        // George Clinton
        {
            name: 'George Clinton',
            data: [
                {
                    x: 'Vice President',
                    y: [
                        new Date(1805, 2, 4).getTime(),
                        new Date(1812, 3, 20).getTime()
                    ]
                }
            ]
        },
        // John Jay
        {
            name: 'John Jay',
            data: [
                {
                    x: 'Secretary of State',
                    y: [
                        new Date(1789, 8, 25).getTime(),
                        new Date(1790, 2, 22).getTime()
                    ]
                }
            ]
        },
        // Edmund Randolph
        {
            name: 'Edmund Randolph',
            data: [
                {
                    x: 'Secretary of State',
                    y: [
                        new Date(1794, 0, 2).getTime(),
                        new Date(1795, 7, 20).getTime()
                    ]
                }
            ]
        },
        // Timothy Pickering
        {
            name: 'Timothy Pickering',
            data: [
                {
                    x: 'Secretary of State',
                    y: [
                        new Date(1795, 7, 20).getTime(),
                        new Date(1800, 4, 12).getTime()
                    ]
                }
            ]
        },
        // Charles Lee
        {
            name: 'Charles Lee',
            data: [
                {
                    x: 'Secretary of State',
                    y: [
                        new Date(1800, 4, 13).getTime(),
                        new Date(1800, 5, 5).getTime()
                    ]
                }
            ]
        },
        // John Marshall
        {
            name: 'John Marshall',
            data: [
                {
                    x: 'Secretary of State',
                    y: [
                        new Date(1800, 5, 13).getTime(),
                        new Date(1801, 2, 4).getTime()
                    ]
                }
            ]
        }
    ];

    const options = {
        chart: {
            height: 350,
            type: 'rangeBar',
            toolbar: {
                show: false,
            }
        },
        plotOptions: {
            bar: {
                horizontal: true,
                barHeight: '35%',
                rangeBarGroupRows: true
            },
        },
        fill: {
            type: 'solid'
        },
        legend: {
            position: 'right'
        },
        xaxis: {
            type: "datetime",
        },
        tooltip: {
            custom: function (opts) {
                const fromYear = new Date(opts.y1).getFullYear()
                const toYear = new Date(opts.y2).getFullYear()
                const values = opts.ctx.rangeBar.getTooltipValues(opts)

                return (
                    '<div class="apexcharts-tooltip-rangebar">' +
                    '<div> <span class="series-name" style="color: ' +
                    values.color +
                    '">' +
                    (values.seriesName ? values.seriesName : '') +
                    '</span></div>' +
                    '<div> <span class="category">' +
                    values.ylabel +
                    ' </span> <span class="value start-value">' +
                    fromYear +
                    '</span> <span class="separator">-</span> <span class="value end-value">' +
                    toYear +
                    '</span></div>' +
                    '</div>'
                )
            }
        },
        colors: chartMultiSeriesColors,
    };

    return (
        <React.Fragment>
            <ReactApexChart dir="ltr"
                className="apex-charts"
                options={options}
                series={series}
                type="rangeBar"
                height={350}
            />
        </React.Fragment>
    );
};
export {
    Basic,
    DifferentColor,
    MultiSeries,
    Advanced,
    MultipleSeries
};