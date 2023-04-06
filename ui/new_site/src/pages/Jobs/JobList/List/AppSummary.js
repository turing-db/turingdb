import React from "react";
import ReactApexChart from "react-apexcharts";
import getChartColorsArray from "../../../../Components/Common/ChartsDynamicColor";

const AppSummaryChart = ({ dataColors }) => {
  var chartDonutBasicColors = getChartColorsArray(dataColors);
  var series = [98, 63, 35];
  var options = {
    labels: ["New Application", "Approved", "Rejected"],
    chart: {
      height: 300,
      type: "donut",
    },
    legend: {
      position: "bottom",
    },
    dataLabels: {
      dropShadow: {
        enabled: false,
      },
    },
    colors: chartDonutBasicColors,
  };
  return (
    <React.Fragment>
      <ReactApexChart dir="ltr"
        className="apex-charts"
        series={series}
        options={options}
        type="donut"
        height={300}
      />
    </React.Fragment>
  );
};

export default AppSummaryChart;
