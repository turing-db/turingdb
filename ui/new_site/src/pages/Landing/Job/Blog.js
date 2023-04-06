import React from "react";
import { Link, NavLink } from "react-router-dom";
import { Button, Card, CardBody, Col, Container, Row } from "reactstrap";
import img8 from "../../../assets/images/small/img-8.jpg";
import img6 from "../../../assets/images/small/img-6.jpg";
import img9 from "../../../assets/images/small/img-9.jpg";
const Blog = () => {
  return (
    <React.Fragment>
      <section className="section" id="blog">
        <Container>
          <Row className="justify-content-center">
            <Col lg={8}>
              <div className="text-center mb-5">
                <h1 className="mb-3 ff-secondary fw-semibold text-capitalize lh-base">
                  Our Latest <span className="text-primary">News</span>
                </h1>
                <p className="text-muted mb-4">
                  We thrive when coming up with innovative ideas but also understand that a smart concept should be supported with faucibus sapien odio measurable results.
                </p>
              </div>
            </Col>
          </Row>

          <Row>
            <Col lg={4} md={6}>
              <Card>
                <CardBody>
                  <img src={img8} alt="" className="img-fluid rounded" />
                </CardBody>
                <CardBody>
                  <ul className="list-inline fs-14 text-muted">
                    <li className="list-inline-item">
                      <i className="ri-calendar-line align-bottom me-1"></i> 30
                      Oct, 2021
                    </li>
                    <li className="list-inline-item">
                      <i className="ri-message-2-line align-bottom me-1"></i>{" "}
                      364 Comment
                    </li>
                  </ul>
                  <Link to="#!">
                    <h5>Design your apps in your own way ?</h5>
                  </Link>
                  <p className="text-muted fs-14">
                    One disadvantage of Lorum Ipsum is that in Latin layouts
                    certain letters appear more frequently than others.
                  </p>

                  <div>
                    <Link to="#!" className="link-success">
                      Learn More{" "}
                      <i className="ri-arrow-right-line align-bottom ms-1"></i>
                    </Link>
                  </div>
                </CardBody>
              </Card>
            </Col>
            <Col lg={4} md={6}>
              <Card>
                <CardBody>
                  <img src={img6} alt="" className="img-fluid rounded" />
                </CardBody>
                <CardBody>
                  <ul className="list-inline fs-14 text-muted">
                    <li className="list-inline-item">
                      <i className="ri-calendar-line align-bottom me-1"></i> 02
                      Oct, 2021
                    </li>
                    <li className="list-inline-item">
                      <i className="ri-message-2-line align-bottom me-1"></i>{" "}
                      245 Comment
                    </li>
                  </ul>
                  <Link to="#!">
                    <h5>Smartest applications for business ?</h5>
                  </Link>
                  <p className="text-muted fs-14">
                    Due to its widespread use as filler text for layouts,
                    non-readability is of great importance: human perception.
                  </p>

                  <div>
                    <Link to="#!" className="link-success">
                      Learn More{" "}
                      <i className="ri-arrow-right-line align-bottom ms-1"></i>
                    </Link>
                  </div>
                </CardBody>
              </Card>
            </Col>
            <Col lg={4} md={6}>
              <Card>
                <CardBody>
                  <img src={img9} alt="" className="img-fluid rounded" />
                </CardBody>
                <CardBody>
                  <ul className="list-inline fs-14 text-muted">
                    <li className="list-inline-item">
                      <i className="ri-calendar-line align-bottom me-1"></i> 23
                      Sept, 2021
                    </li>
                    <li className="list-inline-item">
                      <i className="ri-message-2-line align-bottom me-1"></i>{" "}
                      354 Comment
                    </li>
                  </ul>
                  <Link to="#!">
                    <h5>How apps is changing the IT world</h5>
                  </Link>
                  <p className="text-muted fs-14">
                    Intrinsically incubate intuitive opportunities and real-time
                    potentialities Appropriately communicate one-to-one
                    technology.
                  </p>

                  <div>
                    <Link to="#!" className="link-success">
                      Learn More{" "}
                      <i className="ri-arrow-right-line align-bottom ms-1"></i>
                    </Link>
                  </div>
                </CardBody>
              </Card>
            </Col>
          </Row>
        </Container>
      </section>
      <section className="py-5 bg-primary position-relative">
        <div className="bg-overlay bg-overlay-pattern opacity-50"></div>
        <Container>
          <Row className="align-items-center gy-4">
            <Col className="col-sm">
              <div>
                <h4 className="text-white fw-bold">
                  Get New Jobs Notification!
                </h4>
                <p className="text-white-75 mb-0">
                  Subscribe & get all related jobs notification.
                </p>
              </div>
            </Col>
            <Col className="col-sm-auto">
              <Button className="btn btn-secondary" type="button">
                Subscribe Now{" "}
                <i className="ri-arrow-right-line align-bottom"></i>
              </Button>
            </Col>
          </Row>
        </Container>
      </section>
    </React.Fragment>
  );
};

export default Blog;
