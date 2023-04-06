import React from "react";
import { Card, CardBody, Col, Container, Row } from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import { Link } from "react-router-dom";

import { topCollection } from "../../../common/data";

const Collections = () => {
    document.title = "Collections | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Collections" pageTitle="NFT Marketplace" />
                    <Row className="g-4 mb-3 align-items-center">
                        <Col className="col-sm-auto">
                            <div>
                                <h5 className="mb-0 fw-bold fs-17">Top Collections</h5>
                            </div>
                        </Col>
                        <Col className="col-sm">
                            <div className="d-flex justify-content-sm-end gap-2">
                                <div className="search-box ms-2">
                                    <input type="text" className="form-control" placeholder="Search..." />
                                    <i className="ri-search-line search-icon"></i>
                                </div>
                                <div className="choices">
                                    <select className="form-control w-md">
                                        <option value="All">All</option>
                                        <option value="Today" defaultValue>Today</option>
                                        <option value="Yesterday">Yesterday</option>
                                        <option value="Last 7 Days">Last 7 Days</option>
                                        <option value="Last 30 Days">Last 30 Days</option>
                                        <option value="This Month">This Month</option>
                                        <option value="Last Year">Last Year</option>
                                    </select>
                                </div>
                            </div>
                        </Col>
                    </Row>

                    <Row>
                        {topCollection.map((item, key) => (<Col xl={3} md={4} sm={6} key={key}>
                            <Card>
                                <CardBody>
                                    <Row className="g-1 mb-3">
                                        {item.subItem.map((item, key) => (<Col className="col-6" key={key}>
                                            {item.isChildren.map((item, key) => (<img src={item.img} alt="" className={"img-fluid rounded " + item.imgClass} key={key} />))}
                                        </Col>))}
                                    </Row>
                                    <Link to="#!" className="float-end"> View All <i className="ri-arrow-right-line align-bottom"></i></Link>
                                    <h5 className="mb-0 fs-16"><Link to="#!" className="link-dark">{item.category} <span className="badge badge-soft-success">{item.quantity}</span></Link></h5>
                                </CardBody>
                            </Card>
                        </Col>))}
                    </Row>

                    <Row className="g-0 text-center text-sm-start align-items-center mb-4">
                        <Col sm={6}>
                            <div>
                                <p className="mb-sm-0 text-muted">Showing <span className="fw-semibold">1</span> to <span className="fw-semibold">8</span> of <span className="fw-semibold text-decoration-underline">12</span> entries</p>
                            </div>
                        </Col>

                        <Col sm={6}>
                            <ul className="pagination pagination-separated justify-content-center justify-content-sm-end mb-sm-0">
                                <li className="page-item disabled">
                                    <Link to="#" className="page-link">Previous</Link>
                                </li>
                                <li className="page-item active">
                                    <Link to="#" className="page-link">1</Link>
                                </li>
                                <li className="page-item ">
                                    <Link to="#" className="page-link">2</Link>
                                </li>
                                <li className="page-item">
                                    <Link to="#" className="page-link">3</Link>
                                </li>
                                <li className="page-item">
                                    <Link to="#" className="page-link">4</Link>
                                </li>
                                <li className="page-item">
                                    <Link to="#" className="page-link">5</Link>
                                </li>
                                <li className="page-item">
                                    <Link to="#" className="page-link">Next</Link>
                                </li>
                            </ul>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default Collections;
