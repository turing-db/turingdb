import React from "react";
import { Card, CardBody, CardHeader, Col, Container, Row } from "reactstrap";
import { Link, NavLink } from "react-router-dom";
import ReactApexChart from "react-apexcharts";
import getChartColorsArray from "../../../Components/Common/ChartsDynamicColor";

const NatworkSummary = ({ dataColors }) => {
  var dealTypeChartsColors = getChartColorsArray(dataColors);
  var series = [
    {
      name: "Following",
      data: [80, 50, 30, 40, 100, 20],
    },
    {
      name: "Followers",
      data: [20, 30, 40, 80, 20, 80],
    },
  ];

  var options = {
    chart: {
      height: 341,
      type: "radar",
      dropShadow: {
        enabled: true,
        blur: 1,
        left: 1,
        top: 1,
      },
      toolbar: {
        show: false,
      },
    },
    stroke: {
      width: 2,
    },
    fill: {
      opacity: 0.2,
    },
    legend: {
      show: true,
      fontWeight: 500,
      offsetX: 0,
      offsetY: -8,
      markers: {
        width: 8,
        height: 8,
        radius: 6,
      },
      itemMargin: {
        horizontal: 10,
        vertical: 0,
      },
    },
    markers: {
      size: 0,
    },
    colors: dealTypeChartsColors,
    xaxis: {
      categories: ["2016", "2017", "2018", "2019", "2020", "2021"],
    },
  };
  return (
    <React.Fragment>
      <Col xxl={4} md={6}>
        <Card className="card-height-100">
          <CardHeader className="align-items-center d-flex">
            <h4 className="card-title mb-0 flex-grow-1">
              Your Network Summary
            </h4>
            <div className="flex-shrink-0">
              <div className="dropdown card-header-dropdown">
                <Link
                  className="text-reset dropdown-btn"
                  to="#"
                  data-bs-toggle="dropdown"
                  aria-haspopup="true"
                  aria-expanded="false"
                >
                  <span className="fw-semibold text-uppercase fs-12">
                    Sort by:{" "}
                  </span>
                  <span className="text-muted">
                    Monthly<i className="mdi mdi-chevron-down ms-1"></i>
                  </span>
                </Link>
                <div className="dropdown-menu dropdown-menu-end">
                  <Link className="dropdown-item" to="#">
                    Today
                  </Link>
                  <Link className="dropdown-item" to="#">
                    Weekly
                  </Link>
                  <Link className="dropdown-item" to="#">
                    Monthly
                  </Link>
                  <Link className="dropdown-item" to="#">
                    Yearly
                  </Link>
                </div>
              </div>
            </div>
          </CardHeader>
          <CardBody className="pb-0">
            <ReactApexChart dir="ltr"
              className="apex-charts"
              series={series}
              options={options}
              type="radar"
              height={341}
            />
          </CardBody>
        </Card>
      </Col>
    </React.Fragment>
  );
};

export default NatworkSummary;
