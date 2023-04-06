import React from "react";
import { jobWidgets } from "../../../common/data/appsJobs";
import { Card, Col } from "reactstrap";
import Charts from "./Charts";

const Widgets = () => {
  return (
    <React.Fragment>
      {jobWidgets.map((item, key) => (
        <Col xl={3} md={6} key={key}>
          <Card className="card-height-100">
            <div className="d-flex">
              <div className="flex-grow-1 p-3">
                <h5 className="mb-3">{item.lable}</h5>
                <p className="mb-0 text-muted">
                  <span
                    className={"badge bg-light text-" + item.badgeColor + " mb-0"}
                  >
                    {" "}
                    <i className={item.icon + "align-middle"}></i> {item.number}
                  </span>{" "}
                  vs. previous month
                </p>
              </div>
              <div>
                <Charts seriesData={item.series} dataColors={item.chartColor} />
              </div>
            </div>
          </Card>
        </Col>
      ))}
    </React.Fragment>
  );
};

export default Widgets;
