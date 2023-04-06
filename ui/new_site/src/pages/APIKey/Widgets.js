import React from "react";
import { Card, CardBody, Col, Progress } from "reactstrap";
import FeatherIcon from "feather-icons-react";
import CountUp from "react-countup";

const Widgets = () => {
  return (
    <React.Fragment>
      <Col lg={4}>
        <Card className="card-height-100">
          <CardBody>
            <h5 className="card-title mb-3">Developer Plan</h5>
            <div className="progress animated-progress custom-progress mb-1">
              <Progress
                value={38}
                color="primary"
                className="progress-bar bg-info"
                role="progressbar"
                style={{ width: "38%" }}
                aria-valuenow="38"
                aria-valuemin="0"
                aria-valuemax="100"
              />
            </div>
            <p className="text-muted mb-2">You used 215 of 2000 of your API</p>
            <div className="text-end">
              <button
                type="button"
                className="btn btn-primary btn-sm create-btn"
                data-bs-toggle="modal"
                data-bs-target="#api-key-modal"
              >
                Create API Key
              </button>
            </div>
          </CardBody>
        </Card>
      </Col>
      <Col lg={4}>
        <Card className="card-animate card-height-100">
          <CardBody>
            <div className="d-flex justify-content-between">
              <div>
                <p className="fw-semibold text-muted mb-0">
                  Successful conversions
                </p>
                <h2 className="mt-4 ff-secondary fw-semibold">
                  <CountUp start={0} end={50} duration={3} />
                </h2>
                <p className="mb-0 text-muted">
                  <span className="badge bg-light text-success mb-0">
                    <i className="ri-arrow-up-line align-middle"></i> 8.24 %{" "}
                  </span>{" "}
                  7 last week
                </p>
              </div>
              <div>
                <div className="avatar-sm flex-shrink-0">
                  <span className="avatar-title bg-soft-success rounded-circle fs-2">
                    <FeatherIcon
                      icon="check-circle"
                      className="text-success"
                    ></FeatherIcon>
                  </span>
                </div>
              </div>
            </div>
          </CardBody>
        </Card>
      </Col>
      <Col lg={4}>
        <Card className="card-animate card-height-100">
          <CardBody>
            <div className="d-flex justify-content-between">
              <div>
                <p className="fw-semibold text-muted mb-0">
                  Failed conversions
                </p>
                <h2 className="mt-4 ff-secondary fw-semibold">
                  <CountUp start={0} end={8} duration={3} />
                </h2>
                <p className="mb-0 text-muted">
                  <span className="badge bg-light text-danger mb-0">
                    <i className="ri-arrow-down-line align-middle"></i> 25.87 %{" "}
                  </span>{" "}
                  7 last week
                </p>
              </div>
              <div>
                <div className="avatar-sm flex-shrink-0">
                  <span className="avatar-title bg-soft-danger rounded-circle fs-2">
                    <FeatherIcon
                      icon="alert-octagon"
                      className="text-danger"
                    ></FeatherIcon>
                  </span>
                </div>
              </div>
            </div>
          </CardBody>
        </Card>
      </Col>
    </React.Fragment>
  );
};

export default Widgets;
