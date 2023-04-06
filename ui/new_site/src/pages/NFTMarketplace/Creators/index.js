import React from "react";
import { Card, CardBody, Col, Container, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledDropdown } from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import { Link } from "react-router-dom";

import { creatorsData, creatorsListData } from "../../../common/data/index";

const Creators = () => {
    document.title = "Creators | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Creators" pageTitle="NFT Marketplace" />
                    <Row className="g-4 mb-3">
                        <Col className="col-sm-auto">
                            <div>
                                <Link to="/apps-projects-create" className="btn btn-success"><i className="ri-add-line align-bottom me-1"></i> Add New</Link>
                            </div>
                        </Col>
                        <Col className="col-sm">
                            <div className="d-flex justify-content-sm-end gap-2">
                                <div className="search-box ms-2">
                                    <input type="text" className="form-control" placeholder="Search..." />
                                    <i className="ri-search-line search-icon"></i>
                                </div>
                                <div className="choices">
                                    <select className="form-control w-md" data-choices data-choices-search-false>
                                        <option value="All">All</option>
                                        <option value="Today">Today</option>
                                        <option value="Yesterday" defaultValue>Yesterday</option>
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
                        {creatorsData.map((item, key) => (
                            <Col key={key} xl={3} lg={4} md={6}>
                                <Card>
                                    <CardBody>
                                        <div className="d-flex">
                                            <div className="flex-shrink-0">
                                                <img src={item.img} alt="" className="avatar-sm object-cover rounded" />
                                            </div>
                                            <div className="ms-3 flex-grow-1">
                                                <Link to="/pages-profile">
                                                    <h5 className="mb-1">{item.title}</h5>
                                                </Link>
                                                <p className="text-muted mb-0"><i className="mdi mdi-ethereum text-primary fs-14"></i> {item.price}</p>
                                            </div>
                                            <div>
                                                <UncontrolledDropdown direction="start" className="float-end">
                                                    <DropdownToggle tag="button" className="btn btn-ghost-primary btn-icon">
                                                        <i className="ri-more-fill align-middle fs-16"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu className="dropdown-menu-end">
                                                        <DropdownItem className="view-item-btn">Share</DropdownItem>
                                                        <DropdownItem className="edit-item-btn">Report</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </div>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
                        ))}
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <h5 className="card-title mb-4 fw-bold fs-17">Creators Grid Lists</h5>
                        </Col>
                    </Row>

                    <Row className="row-cols-xl-5 row-cols-lg-3 row-cols-md-2 row-cols-1">
                        {creatorsListData.map((item, key) => (
                            <Col key={key}>
                                <Card>
                                    <img src={item.cardImg} alt="" className="object-cover card-img-top" height="120" />
                                    <div className="card-body text-center">
                                        <img src={item.img} alt="" className="avatar-md rounded-circle object-cover mt-n5 img-thumbnail border-light mx-auto d-block" />
                                        <Link to="/pages-profile">
                                            <h5 className="mt-2 mb-1">{item.title}</h5>
                                        </Link>
                                        <p className="text-muted mb-2">{item.products} Products</p>
                                        <p className="text-muted">You can make an NFT of a digital painting, a text, a piece of music, a video.</p>
                                        <button className={item.isFollowBtn ? "btn btn-success w-100" : "btn btn-soft-success w-100"}>{item.isFollowBtn ? "Follow" : "Unfollow"} </button>
                                    </div>
                                </Card>
                            </Col>
                        ))}
                    </Row>


                    <Row className="g-0 text-center text-sm-start align-items-center mb-4">
                        <Col sm={6}>
                            <div>
                                <p className="mb-sm-0 text-muted">Showing <span className="fw-semibold">1</span> to <span className="fw-semibold">10</span> of <span className="fw-semibold text-decoration-underline">12</span> entries</p>
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

export default Creators;