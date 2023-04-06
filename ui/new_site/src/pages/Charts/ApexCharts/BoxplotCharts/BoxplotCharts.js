import React from "react";
import ReactApexChart from "react-apexcharts";
import getChartColorsArray from "../../../../Components/Common/ChartsDynamicColor";

// Candlestick Chart 
const Basic = ({ dataColors }) => {
  var chartBoxBasicColors = getChartColorsArray(dataColors);
  const series = [
    {
      type: "boxPlot",
      data: [
        {
          x: "Jan 2015",
          y: [54, 66, 69, 75, 88],
        },
        {
          x: "Jan 2016",
          y: [43, 65, 69, 76, 81],
        },
        {
          x: "Jan 2017",
          y: [31, 39, 45, 51, 59],
        },
        {
          x: "Jan 2018",
          y: [39, 46, 55, 65, 71],
        },
        {
          x: "Jan 2019",
          y: [29, 31, 35, 39, 44],
        },
        {
          x: "Jan 2020",
          y: [41, 49, 58, 61, 67],
        },
        {
          x: "Jan 2021",
          y: [54, 59, 66, 71, 88],
        },
      ],
    },
  ];
  var options = {
    chart: {
      type: 'boxPlot',
      height: 350,
      toolbar: {
        show: false
      }
    },
    title: {
      text: 'Basic BoxPlot Chart',
      align: 'left',
      style: {
        fontWeight: 500,
      },
    },
    plotOptions: {
      boxPlot: {
        colors: {
          upper: chartBoxBasicColors[0],
          lower: chartBoxBasicColors[1]
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
        type="boxPlot"
        height={350}
      />
    </React.Fragment>
  );
};

// Scatter Chart
const Scatter = ({ dataColors }) => {
  var chartBoxBasicColors = getChartColorsArray(dataColors);
  const series = [
    {
      name: "Box",
      type: "boxPlot",
      data: [
        {
          x: new Date("2017-01-01").getTime(),
          y: [54, 66, 69, 75, 88]
        },
        {
          x: new Date("2018-01-01").getTime(),
          y: [43, 65, 69, 76, 81]
        },
        {
          x: new Date("2019-01-01").getTime(),
          y: [31, 39, 45, 51, 59]
        },
        {
          x: new Date("2020-01-01").getTime(),
          y: [39, 46, 55, 65, 71]
        },
        {
          x: new Date("2021-01-01").getTime(),
          y: [29, 31, 35, 39, 44]
        }
      ]
    },
    {
      name: "Outliers",
      type: "scatter",
      data: [
        {
          x: new Date("2017-01-01").getTime(),
          y: 32
        },
        {
          x: new Date("2018-01-01").getTime(),
          y: 25
        },
        {
          x: new Date("2019-01-01").getTime(),
          y: 64
        },
        {
          x: new Date("2020-01-01").getTime(),
          y: 27
        },
        {
          x: new Date("2020-01-01").getTime(),
          y: 78
        },
        {
          x: new Date("2021-01-01").getTime(),
          y: 15
        }
      ]
    }
  ];
  var options = {
    chart: {
      toolbar: {
        show: !1
      }
    },
    colors: [chartBoxBasicColors[0], chartBoxBasicColors[1]],
    title: {
      text: "BoxPlot - Scatter Chart",
      align: "left",
      style: {
        fontWeight: 500,
      },
    },
    xaxis: {
      type: "datetime",
      tooltip: {
        formatter: function (val) {
          return new Date(val).getFullYear();
        }
      }
    },
    plotOptions: {
      boxPlot: {
        colors: {
          upper: chartBoxBasicColors[2],
          lower: chartBoxBasicColors[3]
        }
      }
    },
    stroke: {
      colors: [chartBoxBasicColors[4]]
  },
    tooltip: {
      shared: !1,
      intersect: !0
    }
  };

  return (
    <React.Fragment>
      <ReactApexChart dir="ltr"
        className="apex-charts"
        options={options}
        series={series}
        type="boxPlot"
        height={350}
      />
    </React.Fragment>
  );
};

// Horizontal BoxPlot
// Horizontal BoxPlot
const Horizontal = ({ dataColors }) => {
  var chartBoxBasicColors = getChartColorsArray(dataColors);
  const series = [
    {
      type: "boxPlot",
      data: [
        {
          x: 'Category A',
          y: [54, 66, 69, 75, 88]
        },
        {
          x: 'Category B',
          y: [43, 65, 69, 76, 81]
        },
        {
          x: 'Category C',
          y: [31, 39, 45, 51, 59]
        },
        {
          x: 'Category D',
          y: [39, 46, 55, 65, 71]
        },
        {
          x: 'Category E',
          y: [29, 31, 35, 39, 44]
        },
        {
          x: 'Category F',
          y: [41, 49, 58, 61, 67]
        },
        {
          x: 'Category G',
          y: [54, 59, 66, 71, 88]
        }
      ],
    },
  ];
  var options = {
    chart: {
      type: 'boxPlot',
      height: 350,
      toolbar: {
        show: false
      }
    },
    plotOptions: {
      bar: {
        horizontal: true,
        barHeight: '50%'
      },
      boxPlot: {
        colors: {
          upper: chartBoxBasicColors[0],
          lower: chartBoxBasicColors[1]
        },
      },
    },
    stroke: {
      colors: [chartBoxBasicColors[2]]
    },
  };
  return (
    <React.Fragment>
      <ReactApexChart dir="ltr"
        className="apex-charts"
        options={options}
        series={series}
        type="boxPlot"
        height={350}
      />
    </React.Fragment>
  );
};


export {
  Basic,
  Scatter,
  Horizontal
};
