import React from "react";
import { Card, CardBody, Col, Container, Row } from "reactstrap";

import Avatar10 from "../../../assets/images/users/avatar-10.jpg";
import About from "../../../assets/images/about.jpg";
import { NavLink } from "react-router-dom";

const Features = () => {
  return (
    <React.Fragment>
      <section className="section">
        <Container>
          <Row className="align-items-center justify-content-lg-between justify-content-center gy-4">
            <Col lg={5} sm={7}>
              <div className="about-img-section mb-5 mb-lg-0 text-center">
                <Card className="rounded shadow-lg inquiry-box d-none d-lg-block">
                  <CardBody className="d-flex align-items-center">
                    <div className="avatar-sm flex-shrink-0 me-3">
                      <div className="avatar-title bg-soft-info text-info rounded-circle fs-18">
                        <i className="ri-briefcase-2-line"></i>
                      </div>
                    </div>
                    <h5 className="fs-15 lh-base mb-0">
                      Search Over{" "}
                      <span className="text-secondary fw-bold">1,00,000+</span>{" "}
                      Jobs
                    </h5>
                  </CardBody>
                </Card>

                <Card className="feedback-box">
                  <CardBody className="d-flex shadow-lg">
                    <div className="flex-shrink-0 me-3">
                      <img
                        src={Avatar10}
                        alt=""
                        className="avatar-sm rounded-circle"
                      />
                    </div>
                    <div className="flex-grow-1">
                      <h5 className="fs-14 lh-base mb-0">Tonya Noble</h5>
                      <p className="text-muted fs-11 mb-1">UI/UX Designer</p>

                      <div className="text-warning">
                        <i className="ri-star-s-fill"></i>
                        <i className="ri-star-s-fill"></i>
                        <i className="ri-star-s-fill"></i>
                        <i className="ri-star-s-fill"></i>
                        <i className="ri-star-s-line"></i>
                      </div>
                    </div>
                  </CardBody>
                </Card>
                <img
                  src={About}
                  alt=""
                  className="img-fluid mx-auto rounded-3"
                />
              </div>
            </Col>
            <Col lg={6}>
              <div className="text-muted">
                <h1 className="mb-3 fw-bold lh-base">
                  Find Your <span className="text-primary">Dream Job</span> in
                  One Place
                </h1>
                <p className="ff-secondary fs-16 mb-2">
                  The first step in finding your <b>dream job </b> is deciding
                  where to look for first-hand insight. Contact professionals
                  who are already working in industries or positions that
                  interest you.
                </p>
                <p className="ff-secondary fs-16">
                  Schedule informational interviews and phone calls or ask for
                  the opportunity to shadow them on the job.
                </p>

                <div className="vstack gap-2 mb-4 pb-1">
                  <div className="d-flex align-items-center">
                    <div className="flex-shrink-0 me-2">
                      <div className="avatar-xs icon-effect">
                        <div className="avatar-title bg-transparent text-success rounded-circle h2">
                          <i className="ri-check-fill"></i>
                        </div>
                      </div>
                    </div>
                    <div className="flex-grow-1">
                      <p className="mb-0">Dynamic Conetnt</p>
                    </div>
                  </div>
                  <div className="d-flex align-items-center">
                    <div className="flex-shrink-0 me-2">
                      <div className="avatar-xs icon-effect">
                        <div className="avatar-title bg-transparent text-success rounded-circle h2">
                          <i className="ri-check-fill"></i>
                        </div>
                      </div>
                    </div>
                    <div className="flex-grow-1">
                      <p className="mb-0">Setup plugin's information.</p>
                    </div>
                  </div>
                  <div className="d-flex align-items-center">
                    <div className="flex-shrink-0 me-2">
                      <div className="avatar-xs icon-effect">
                        <div className="avatar-title bg-transparent text-success rounded-circle h2">
                          <i className="ri-check-fill"></i>
                        </div>
                      </div>
                    </div>
                    <div className="flex-grow-1">
                      <p className="mb-0">Themes customization information</p>
                    </div>
                  </div>
                </div>

                <div>
                  <NavLink to="#!" className="btn btn-secondary">
                    Find Your Jobs{" "}
                    <i className="ri-arrow-right-line align-bottom ms-1"></i>
                  </NavLink>
                </div>
              </div>
            </Col>
          </Row>
        </Container>
      </section>
    </React.Fragment>
  );
};

export default Features;
