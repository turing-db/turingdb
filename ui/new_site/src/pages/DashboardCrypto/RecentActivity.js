import React from 'react';
import { Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';

//Import Icons
import FeatherIcon from "feather-icons-react";

//SimpleBar
import SimpleBar from "simplebar-react";
import { Link } from 'react-router-dom';

const RecentActivity = () => {
    return (
        <React.Fragment>
            <Col xxl={4} lg={5}>
                <Card className="card-height-100">
                    <CardHeader className="card-header align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Recent Activity</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown">
                                <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                    <span className="fw-semibold text-uppercase fs-13">Sort by: </span><span className="text-muted">Current Week<i className="mdi mdi-chevron-down ms-1"></i></span>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu-end">
                                    <DropdownItem>Today</DropdownItem>
                                    <DropdownItem>Last Week</DropdownItem>
                                    <DropdownItem>Last Month</DropdownItem>
                                    <DropdownItem>Current Year</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </CardHeader>
                    <CardBody className="p-0">
                        <SimpleBar style={{ height: "390px" }}>
                            <div className="p-3">
                                <h6 className="text-muted text-uppercase mb-3 fs-11">25 Dec 2021</h6>
                                <div className="d-flex align-items-center">
                                    <div className="avatar-xs flex-shrink-0">
                                        <span className="avatar-title bg-light rounded-circle">
                                            <FeatherIcon
                                                icon="arrow-down-circle"
                                                className="icon-dual-success icon-sm"
                                            />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Bought Bitcoin</h6>
                                        <p className="text-muted fs-13 mb-0">
                                            <i className="mdi mdi-circle-medium text-success fs-15 align-middle"></i>
                                            Visa Debit Card ***6
                                        </p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1 text-success">+0.04025745<span className="text-uppercase ms-1">Btc</span></h6>
                                        <p className="text-muted fs-13 mb-0">+878.52 USD</p>
                                    </div>
                                </div>
                                <div className="d-flex align-items-center mt-3">
                                    <div className="avatar-xs flex-shrink-0">
                                        <span className="avatar-title bg-light rounded-circle">
                                            <FeatherIcon
                                                icon="send"
                                                className="icon-dual-warning icon-sm"
                                            />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Sent Eathereum</h6>
                                        <p className="text-muted fs-13 mb-0">
                                            <i className="mdi mdi-circle-medium text-warning fs-15 align-middle"></i>
                                            Sofia Cunha
                                        </p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1 text-muted">-0.09025182<span className="text-uppercase ms-1">Eth</span></h6>
                                        <p className="text-muted fs-13 mb-0">-659.35 USD</p>
                                    </div>
                                </div>

                                <h6 className="text-muted text-uppercase mt-4 mb-3 fs-11">24 Dec 2021</h6>
                                <div className="d-flex align-items-center">
                                    <div className="avatar-xs flex-shrink-0">
                                        <span className="avatar-title bg-light rounded-circle">
                                            <FeatherIcon
                                                icon="arrow-up-circle"
                                                className="icon-dual-danger icon-sm"
                                            />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Sell Dash</h6>
                                        <p className="text-muted fs-13 mb-0">
                                            <i className="mdi mdi-circle-medium text-danger fs-15 align-middle"></i>
                                            www.cryptomarket.com
                                        </p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1 text-danger">-98.6025422<span className="text-uppercase ms-1">Dash</span></h6>
                                        <p className="text-muted fs-13 mb-0">-1508.98 USD</p>
                                    </div>
                                </div>
                                <div className="d-flex align-items-center mt-3">
                                    <div className="avatar-xs flex-shrink-0">
                                        <span className="avatar-title bg-light rounded-circle">
                                            <FeatherIcon
                                                icon="arrow-up-circle"
                                                className="icon-dual-danger icon-sm"
                                            />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Sell Dogecoin</h6>
                                        <p className="text-muted fs-13 mb-0">
                                            <i className="mdi mdi-circle-medium text-success fs-15 align-middle"></i>
                                            www.coinmarket.com
                                        </p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1 text-danger">-1058.08025142<span className="text-uppercase ms-1">Doge</span></h6>
                                        <p className="text-muted fs-13 mb-0">-89.36 USD</p>
                                    </div>
                                </div>
                                <div className="d-flex align-items-center mt-3">
                                    <div className="avatar-xs flex-shrink-0">
                                        <span className="avatar-title bg-light rounded-circle">
                                            <FeatherIcon
                                                icon="arrow-up-circle"
                                                className="icon-dual-success icon-sm"
                                            />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Bought Litecoin</h6>
                                        <p className="text-muted fs-13 mb-0">
                                            <i className="mdi mdi-circle-medium text-warning fs-15 align-middle"></i>
                                            Payment via Wallet
                                        </p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1 text-success">+0.07225912<span className="text-uppercase ms-1">Ltc</span></h6>
                                        <p className="text-muted fs-13 mb-0">+759.45 USD</p>
                                    </div>
                                </div>

                                <h6 className="text-muted text-uppercase mb-3 mt-4 fs-11">20 Dec 2021</h6>
                                <div className="d-flex align-items-center">
                                    <div className="avatar-xs flex-shrink-0">
                                        <span className="avatar-title bg-light rounded-circle">
                                            <FeatherIcon
                                                icon="send"
                                                className="icon-dual-warning icon-sm"
                                            />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Sent Eathereum</h6>
                                        <p className="text-muted fs-13 mb-0">
                                            <i className="mdi mdi-circle-medium text-warning fs-15 align-middle"></i>
                                            Sofia Cunha
                                        </p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1 text-muted">-0.09025182<span className="text-uppercase ms-1">Eth</span></h6>
                                        <p className="text-muted fs-13 mb-0">-659.35 USD</p>
                                    </div>
                                </div>

                                <div className="d-flex align-items-center mt-3">
                                    <div className="avatar-xs flex-shrink-0">
                                        <span className="avatar-title bg-light rounded-circle">
                                            <FeatherIcon
                                                icon="arrow-down-circle"
                                                className="icon-dual-success icon-sm"
                                            />
                                        </span>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Bought Bitcoin</h6>
                                        <p className="text-muted fs-13 mb-0">
                                            <i className="mdi mdi-circle-medium text-success fs-15 align-middle"></i>
                                            Visa Debit Card ***6
                                        </p>
                                    </div>
                                    <div className="flex-shrink-0 text-end">
                                        <h6 className="mb-1 text-success">+0.04025745<span className="text-uppercase ms-1">Btc</span></h6>
                                        <p className="text-muted fs-13 mb-0">+878.52 USD</p>
                                    </div>
                                </div>

                                <div className="mt-3 text-center">
                                    <Link to="#" className="text-muted text-decoration-underline">Load More</Link>
                                </div>

                            </div>

                        </SimpleBar>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default RecentActivity;