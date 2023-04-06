import React from 'react';
import { Card, CardBody, Col, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledDropdown } from 'reactstrap';
import CountUp from "react-countup";

// Import Images
import Bgd from "../../assets/images/bg-d.png";
import { Link } from 'react-router-dom';

const Widgets = () => {
    return (
        <React.Fragment>
            <Row>
                <Col xl={6}>
                    <Card className="overflow-hidden">
                        <CardBody className="bg-marketplace d-flex">
                            <div className="flex-grow-1">
                                <h4 className="fs-18 lh-base mb-0">Discover, Collect, Sell and Create <br /> your own <span className="text-success">NFTs.</span> </h4>
                                <p className="mb-0 mt-2 pt-1 text-muted">The world's first and largest digital marketplace.</p>
                                <div className="d-flex gap-3 mt-4">
                                    <Link to="/#" className="btn btn-secondary">Discover Now </Link>
                                    <Link to="/#" className="btn btn-danger">Create Your Own</Link>
                                </div>
                            </div>
                            <img src={Bgd} alt="" className="img-fluid" />
                        </CardBody>
                    </Card>
                </Col>
                <Col xl={3} md={6}>
                    <Card className="card-height-100">
                        <CardBody>
                            <div className="float-end">
                                <UncontrolledDropdown className="card-header-dropdown">
                                    <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                        <span className="text-muted fs-18"><i className="mdi mdi-dots-vertical align-middle"></i></span>
                                    </DropdownToggle>
                                    <DropdownMenu className="dropdown-menu-end">
                                        <DropdownItem>Today</DropdownItem>
                                        <DropdownItem>Last Week</DropdownItem>
                                        <DropdownItem>Last Month</DropdownItem>
                                        <DropdownItem>Current Year</DropdownItem>
                                    </DropdownMenu>
                                </UncontrolledDropdown>
                            </div>
                            <div className="d-flex align-items-center">
                                <div className="avatar-sm flex-shrink-0">
                                    <span className="avatar-title bg-soft-info rounded fs-3">
                                        <i className="bx bx-dollar-circle text-info"></i>
                                    </span>
                                </div>
                                <div className="flex-grow-1 ps-3">
                                    <h5 className="text-muted text-uppercase fs-13 mb-0">Total Revenue</h5>
                                </div>
                            </div>
                            <div className="mt-4 pt-1">
                                <h4 className="fs-22 fw-semibold ff-secondary mb-0">$
                                    <span className="counter-value">
                                        <CountUp start={0} end={559526.564} separator={","} decimals={3} duration={4} />
                                    </span>
                                </h4>
                                <p className="mt-4 mb-0 text-muted"><span className="badge bg-soft-danger text-danger mb-0 me-1"> <i className="ri-arrow-down-line align-middle"></i> 3.96 % </span> vs. previous month</p>
                            </div>
                        </CardBody>
                    </Card>
                </Col>
                <Col xl={3} md={6}>
                    <Card className="card-height-100">
                        <CardBody>
                            <div className="float-end">
                                <UncontrolledDropdown className="card-header-dropdown">
                                    <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                        <span className="text-muted fs-18"><i className="mdi mdi-dots-vertical align-middle"></i></span>
                                    </DropdownToggle>
                                    <DropdownMenu className="dropdown-menu-end">
                                        <DropdownItem>Today</DropdownItem>
                                        <DropdownItem>Last Week</DropdownItem>
                                        <DropdownItem>Last Month</DropdownItem>
                                        <DropdownItem>Current Year</DropdownItem>
                                    </DropdownMenu>
                                </UncontrolledDropdown>
                            </div>
                            <div className="d-flex align-items-center">
                                <div className="avatar-sm flex-shrink-0">
                                    <span className="avatar-title bg-soft-info rounded fs-3">
                                        <i className="bx bx-wallet text-info"></i>
                                    </span>
                                </div>
                                <div className="flex-grow-1 ps-3">
                                    <h5 className="text-muted text-uppercase fs-13 mb-0">Estimated</h5>
                                </div>
                            </div>
                            <div className="mt-4 pt-1">
                                <h4 className="fs-22 fw-semibold ff-secondary mb-0">$
                                    <span className="counter-value">
                                        <CountUp start={0} end={624562.564} separator={","} decimals={3} duration={4} />
                                    </span>
                                </h4>
                                <p className="mt-4 mb-0 text-muted"><span className="badge bg-soft-success text-success mb-0"> <i className="ri-arrow-up-line align-middle"></i> 16.24 % </span> vs. previous month</p>
                            </div>
                        </CardBody>
                    </Card>
                </Col>
            </Row>
        </React.Fragment>
    );
};

export default Widgets;