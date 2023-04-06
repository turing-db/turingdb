import React from "react";
import { Link, NavLink } from "react-router-dom";
import { Card, CardBody, Col, Container, Row } from "reactstrap";
import { categories } from "../../../common/data/jobLanding";

const Categories = () => {
  return (
    <React.Fragment>
      <section className="section bg-light" id="categories">
        <Container>
          <Row className="justify-content-center">
            <Col lg={7}>
              <div className="text-center mb-5">
                <h1 className="mb-3 ff-secondary fw-semibold text-capitalize lh-base">
                  High demand jobs{" "}
                  <span className="text-primary">Categories</span> fratured
                </h1>
                <p className="text-muted">
                  Post a job to tell us about your project. We'll quickly match
                  you with the right freelancers.
                </p>
              </div>
            </Col>
          </Row>

          <Row className="justify-content-center">
            {categories.map((item, key) => (
              <Col lg={3} md={6} key={key}>
                <Card className="shadow-none text-center py-3">
                  <CardBody className="py-4">
                    <div className="avatar-sm position-relative mb-4 mx-auto">
                      <div className="job-icon-effect"></div>
                      <div className="avatar-title bg-transparent text-success rounded-circle">
                        <i className={item.icon}></i>
                      </div>
                    </div>
                    <Link to="#!" className="to-link">
                      <h5 className="fs-17 pt-1">{item.lable}</h5>
                    </Link>
                    <p className="mb-0 text-muted">{item.job}</p>
                  </CardBody>
                </Card>
              </Col>
            ))}
          </Row>
        </Container>
      </section>
      <section className="py-5 bg-primary position-relative">
        <div className="bg-overlay bg-overlay-pattern opacity-50"></div>
        <Container>
          <Row className="align-items-center gy-4">
            <Col className="col-sm">
              <div>
                <h4 className="text-white fw-bold mb-2">Ready to Started?</h4>
                <p className="text-white-50 mb-0">
                  Create new account and refer your friend
                </p>
              </div>
            </Col>
            <Col className="col-sm-auto">
              <div>
                <Link to="#!" className="btn bg-gradient btn-danger">
                  Create Free Account
                </Link>
              </div>
            </Col>
          </Row>
        </Container>
      </section>
    </React.Fragment>
  );
};

export default Categories;
