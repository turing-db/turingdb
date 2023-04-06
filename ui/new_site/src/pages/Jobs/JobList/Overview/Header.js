import React from "react";
import { Card, CardBody, Col, Row } from "reactstrap";

import Slack from "../../../../assets/images/brands/slack.png";
const Header = () => {
  return (
    <React.Fragment>
      <Col lg={12}>
        <Card className="mt-n4 mx-n4">
          <div className="bg-soft-secondary">
            <CardBody className="px-4 pb-4">
              <Row className="mb-3">
                <Col className="col-md">
                  <Row className="row align-items-center g-3">
                    <Col className="col-md-auto">
                      <div className="avatar-md">
                        <div className="avatar-title bg-white rounded-circle">
                          <img src={Slack} alt="" className="avatar-xs" />
                        </div>
                      </div>
                    </Col>
                    <Col className="col-md">
                      <div>
                        <h4 className="fw-bold">Product Designer</h4>
                        <div className="hstack gap-3 flex-wrap">
                          <div>
                            <i className="ri-building-line align-bottom me-1"></i>{" "}
                            Themesbrand
                          </div>
                          <div className="vr"></div>
                          <div>
                            <i className="ri-map-pin-2-line align-bottom me-1"></i>{" "}
                            Zuweihir, UAE
                          </div>
                          <div className="vr"></div>
                          <div>
                            Post Date :{" "}
                            <span className="fw-semibold">15 Sep, 2021</span>
                          </div>
                          <div className="vr"></div>
                          <div className="badge rounded-pill bg-success fs-12">
                            Full Time
                          </div>
                        </div>
                      </div>
                    </Col>
                  </Row>
                </Col>

                <Col className="col-md-auto">
                  <div className="hstack gap-1 flex-wrap mt-4 mt-md-0">
                    <button
                      type="button"
                      className="btn btn-icon btn-sm btn-ghost-warning fs-16"
                    >
                      <i className="ri-star-fill"></i>
                    </button>
                    <button
                      type="button"
                      className="btn btn-icon btn-sm btn-ghost-primary fs-16"
                    >
                      <i className="ri-share-line"></i>
                    </button>
                    <button
                      type="button"
                      className="btn btn-icon btn-sm btn-ghost-primary fs-16"
                    >
                      <i className="ri-flag-line"></i>
                    </button>
                  </div>
                </Col>
              </Row>
            </CardBody>
          </div>
        </Card>
      </Col>
    </React.Fragment>
  );
};

export default Header;
