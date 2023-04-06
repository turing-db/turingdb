import React from "react";
import CountUp from "react-countup";
import { Card, CardBody, Col } from "reactstrap";
import { buysellWidgets } from "../../../common/data";

const Widgets = () => {
  return (
    <React.Fragment>
      {buysellWidgets.map((item, key) => (
        <Col xl={3} sm={6} key={key}>
          <Card className="card-animate">
            <CardBody>
              <div className="d-flex">
                <div className="flex-grow-1">
                  <h6 className="text-muted mb-3">{item.title}</h6>
                  <h2 className="mb-0">
                    $
                    <span className="counter-value">
                      <CountUp start={0} end={item.counter} duration={3} />
                    </span>
                    <small className="text-muted fs-13">.{item.decimal}k</small>
                  </h2>
                </div>
                <div className="flex-shrink-0 avatar-sm">
                  <div
                    className={
                      "avatar-title fs-22 rounded bg-soft-" +
                      item.iconClass +
                      " text-" +
                      item.iconClass
                    }
                  >
                    <i className={item.icon}></i>
                  </div>
                </div>
              </div>
            </CardBody>
          </Card>
        </Col>
      ))}
    </React.Fragment>
  );
};

export default Widgets;
