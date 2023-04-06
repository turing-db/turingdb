import React from "react";
import ReactApexChart from "react-apexcharts";

const SellerChats = (props) => {
  const series = [
    {
      data: props.data,
    },
  ];

  const options = {
    chart: {
      sparkline: {
        enabled: true,
      },
    },
    fill: {
      type: "gradient",
      gradient: {
        shadeIntensity: 1,
        inverseColors: false,
        opacityFrom: 0.45,
        opacityTo: 0.05,
        stops: [20, 100, 100, 100],
      },
    },
    stroke: {
      curve: "smooth",
      width: 2,
    },
    colors: [props.color],
    tooltip: {
      fixed: {
        enabled: false,
      },
      x: {
        show: false,
      },
      y: {
        title: {
          formatter: function (seriesName) {
            return "";
          },
        },
      },
      marker: {
        show: false,
      },
    },
  };

  return (
    <ReactApexChart dir="ltr"
      options={options}
      series={series}
      type="area"
      height="50"
      className="apex-charts"
    />
  );
};

export default SellerChats;
