import React, { useState } from "react";
import { Link } from "react-router-dom";
import { Card, CardBody, Col, Container, Input, Row } from "reactstrap";
import { jobCandidates } from "../../../../common/data/appsJobs";
import BreadCrumb from "../../../../Components/Common/BreadCrumb";

const CandidateList = () => {

  const [isBookmarkClick, setIsBookmarkClick] = useState(false);

  document.title = "Candidate List View | Velzon -  Admin & Dashboard Template";
  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="List View" pageTitle="Candidates Lists" />

          <Row className="g-4 mb-4">
            <Col className="col-sm-auto">
              <div>
                <Link to="#!" className="btn btn-secondary">
                  <i className="ri-add-line align-bottom me-1"></i> Add
                  Candidate
                </Link>
              </div>
            </Col>
            <Col className="col-sm">
              <div className="d-md-flex justify-content-sm-end gap-2">
                <div className="search-box ms-md-2 flex-shrink-0 mb-3 mb-md-0">
                  <Input
                    type="text"
                    className="form-control"
                    id="searchJob"
                    autoComplete="off"
                    placeholder="Search for candidate name or designation..."
                  />
                  <i className="ri-search-line search-icon"></i>
                </div>

                <select
                  className="form-control w-md"
                  data-choices
                  data-choices-search-false
                >
                  <option value="All">All</option>
                  <option value="Today">Today</option>
                  <option value="Yesterday" defaultValue>
                    Yesterday
                  </option>
                  <option value="Last 7 Days">Last 7 Days</option>
                  <option value="Last 30 Days">Last 30 Days</option>
                  <option value="This Month">This Month</option>
                  <option value="Last Year">Last Year</option>
                </select>
              </div>
            </Col>
          </Row>

          <Row className="gy-2 mb-2" id="candidate-list">
            {jobCandidates.map((item, key) => (
              <Col lg={12} key={key}>
                <Card className="mb-0">
                  <CardBody>
                    <div className="d-lg-flex align-items-center">
                      <div className="flex-shrink-0">
                        {item.nickname ? (
                          <div className="avatar-title border bg-light text-primary rounded p-2 text-uppercase fs-16">
                            {item.nickname}
                          </div>
                        ) : (
                          <div className="avatar-sm rounded">
                            <img
                              src={item.userImg}
                              alt=""
                              className="member-img img-fluid d-block rounded"
                            ></img>
                          </div>
                        )}
                      </div>
                      <div className="ms-3">
                        <Link to="/pages-profile">
                          <h5 className="fs-16 mb-2">{item.candidateName}</h5>
                        </Link>
                        <p className="text-muted mb-0">{item.designation}</p>
                      </div>
                      <div className="d-flex gap-4 mt-0 text-muted mx-auto">
                        <div>
                          <i className="ri-map-pin-2-line text-primary me-1 align-bottom"></i>{" "}
                          {item.location}
                        </div>
                        <div>
                          <i className="ri-time-line text-primary me-1 align-bottom"></i>
                          {item.type === "Part Time" ?
                            <span className="badge badge-soft-danger">{item.type}</span>
                            :
                            item.type === "Full Time" ?
                              <span className="badge badge-soft-success">{item.type}</span>
                              :
                              <span className="badge badge-soft-secondary">{item.type}</span>
                          }
                        </div>
                      </div>
                      <div className="d-flex flex-wrap gap-2 align-items-center mx-auto">
                        <div className="badge text-bg-success">
                          <i className="mdi mdi-star me-1"></i>
                          {item.rating[0]}
                        </div>
                        <div className="text-muted">{item.rating[1]}</div>
                      </div>
                      <div>
                        <Link to="#!" className="btn btn-soft-success">
                          View Details
                        </Link>
                        <Link
                          to="#"
                          onClick={(e) => {
                            e.preventDefault();
                            setIsBookmarkClick(!isBookmarkClick);
                          }}
                          className={isBookmarkClick ? "btn btn-ghost-danger btn-icon custom-toggle active" : "btn btn-ghost-danger btn-icon custom-toggle"}
                        >
                          {!isBookmarkClick ?
                            <span className="icon-on">
                              <i className="ri-bookmark-line align-bottom"></i>
                            </span>
                            :
                            <span className="icon-off">
                              <i className="ri-bookmark-3-fill align-bottom"></i>
                            </span>
                          }
                        </Link>
                      </div>
                    </div>
                  </CardBody>
                </Card>
              </Col>
            ))}
          </Row>

          <Row
            className="g-0 justify-content-end mb-4"
            id="pagination-element"
          >
            <Col sm={6}>
              <div className="pagination-block pagination pagination-separated justify-content-center justify-content-sm-end mb-sm-0">
                <div className="page-item">
                  <Link to="" className="page-link" id="page-prev">
                    Previous
                  </Link>
                </div>
                <span id="page-num" className="pagination"></span>
                <div className="page-item">
                  <Link to="" className="page-link" id="page-next">
                    Next
                  </Link>
                </div>
              </div>
            </Col>
          </Row>
        </Container>
      </div>
    </React.Fragment>
  );
};

export default CandidateList;
