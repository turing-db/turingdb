import React from "react";
import { Card, CardHeader, Col, DropdownMenu, DropdownToggle, UncontrolledDropdown, DropdownItem, CardBody } from 'reactstrap';
import ReactApexChart from "react-apexcharts";
import getChartColorsArray from "../../../Components/Common/ChartsDynamicColor";


const VisitorGraph = ({ dataColors }) => {
  var chartTreemapDiffColor = getChartColorsArray(dataColors);

  const series = [
    {
      data: [
        {
          x: "USA",
          y: 321,
        },
        {
          x: "Russia",
          y: 165,
        },
        {
          x: "India",
          y: 184,
        },
        {
          x: "China",
          y: 98,
        },
        {
          x: "Canada",
          y: 84,
        },
        {
          x: "Brazil",
          y: 31,
        },
        {
          x: "UK",
          y: 70,
        },
        {
          x: "Australia",
          y: 30,
        },
        {
          x: "Germany",
          y: 44,
        },
        {
          x: "Italy",
          y: 68,
        },
        {
          x: "Israel",
          y: 28,
        },
        {
          x: "Indonesia",
          y: 19,
        },
        {
          x: "Bangladesh",
          y: 29,
        },
      ],
    },
  ];
  var options = {
    legend: {
      show: false,
    },
    chart: {
      height: 345,
      type: "treemap",
      toolbar: {
        show: false,
      },
    },
    title: {
      text: "Visitors Location",
      align: "center",
      style: {
        fontWeight: 500,
      },
    },
    colors: chartTreemapDiffColor,
    plotOptions: {
      treemap: {
        distributed: true,
        enableShades: false,
      },
    },
  };

  return (
    <React.Fragment>
      <Col xl={8}>
        <Card>
          <CardHeader>
            <div className="d-flex">
              <h5 className="card-title mb-0 flex-grow-1  ">Visitor Graph</h5>
              <div className="flex-shrink-0">
              <UncontrolledDropdown className="card-header-dropdown">
                  <DropdownToggle
                    className="text-reset dropdown-btn"
                    tag="a"
                    type="a"
                    href="#"
                  >
                    <span className="fw-semibold text-uppercase fs-12">
                      Sort by:{" "}
                    </span>
                    <span className="text-muted">
                      Current Week
                      <i className="mdi mdi-chevron-down ms-1"></i>
                    </span>
                  </DropdownToggle>
                  <DropdownMenu className="dropdown-menu-end">
                    <DropdownItem href="#">Today</DropdownItem>
                    <DropdownItem href="#">Last Week</DropdownItem>
                    <DropdownItem href="#">Last Month</DropdownItem>
                    <DropdownItem href="#">Current Year</DropdownItem>
                  </DropdownMenu>
                </UncontrolledDropdown>
              </div>
            </div>
          </CardHeader>
          <CardBody>
            <ReactApexChart dir="ltr"
              className="apex-charts"
              series={series}
              options={options}
              type="treemap"
              height="345"
            />
          </CardBody>
        </Card>
      </Col>
    </React.Fragment>
  );
};

export default VisitorGraph;
