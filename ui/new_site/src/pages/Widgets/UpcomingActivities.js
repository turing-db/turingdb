import React from 'react';
import { Button, Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, ListGroup, ListGroupItem, Pagination, PaginationItem, PaginationLink, Row, UncontrolledDropdown } from 'reactstrap';
import { Link } from 'react-router-dom';

import SimpleBar from 'simplebar-react';

// Import Images

import product2 from "../../assets/images/products/img-2.png";
import product8 from "../../assets/images/products/img-8.png";
import product10 from "../../assets/images/products/img-10.png";

import avatar2 from "../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../assets/images/users/avatar-3.jpg";

import { widgetsActivities, widgetsTasks } from "../../common/data/index";
import { OtherWidgetsCharts } from './WidgetsCharts';

const UpcomingActivity = () => {

    const chartDataOptions = [
        {
            id: 1,
            title: "Application",
            persantage: "16.24",
            series: [{
                name: "Results",
                data: [0, 68, 35, 90, 99],
            }],
            dataColors: '["--vz-success" , "--vz-transparent"]'
        },
        {
            id: 2,
            title: "Interviewed",
            persantage: "34.24",
            series: [{
                name: "Results",
                data: [0, 98, 85, 90, 67],
            }],
            dataColors: '["--vz-danger" , "--vz-transparent"]'
        },
        {
            id: 3,
            title: "Hired",
            persantage: "6.67",
            series: [{
                name: "Results",
                data: [0, 65, 103, 75, 120],
            }],
            dataColors: '["--vz-success" , "--vz-transparent"]'
        },
    ]

    return (
        <React.Fragment>
            <Row>
                <Col xxl={5}>
                    <Card className="card-height-100">
                        <CardHeader className="align-items-center d-flex">
                            <h4 className="card-title mb-0 flex-grow-1">Upcoming Activities</h4>
                            <div className="flex-shrink-0">

                                <UncontrolledDropdown className="card-header-dropdown" direction='start'>
                                    <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                        <span className="text-muted fs-18"><i className="mdi mdi-dots-vertical"></i></span>
                                    </DropdownToggle>
                                    <DropdownMenu className="dropdown-menu-end">
                                        <DropdownItem>Edit</DropdownItem>
                                        <DropdownItem>Remove</DropdownItem>
                                    </DropdownMenu>
                                </UncontrolledDropdown>

                            </div>
                        </CardHeader>

                        <CardBody className="pt-0">
                            <ListGroup className="border-dashed" flush>
                                {widgetsActivities.map((item, key) => (
                                    <ListGroupItem className="ps-0" key={key}>
                                        <Row className="align-items-center g-3">
                                            <Col className="col-auto">
                                                <div className="avatar-sm p-1 py-2 h-auto bg-light rounded-3">
                                                    <div className="text-center">
                                                        <h5 className="mb-0">{item.date}</h5>
                                                        <div className="text-muted">{item.weekDay}</div>
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col className="col">
                                                <h5 className="text-muted mt-0 mb-1 fs-13">{item.time}</h5>
                                                <Link to="#" className="text-reset fs-14 mb-0">{item.caption}</Link>
                                            </Col>
                                            <Col className="col-sm-auto">
                                                <div className="avatar-group">
                                                    {item.subItem.map((item, key) => (
                                                        <React.Fragment key={key}>
                                                            {item.img ? (<div className="avatar-group-item">
                                                                <Link to="#" className="d-inline-block" id={item.targetId}>
                                                                    <img src={item.img} alt="" className="rounded-circle avatar-xxs" />
                                                                </Link>
                                                            </div>) : (<div className="avatar-group-item">
                                                                <Link to="#">
                                                                    <div className="avatar-xxs">
                                                                        <span className={"avatar-title rounded-circle text-white " + item.bgcolor}>
                                                                            {item.imgNumber}
                                                                        </span>
                                                                    </div>
                                                                </Link>
                                                            </div>)
                                                            }
                                                        </React.Fragment>
                                                    ))}
                                                </div>
                                            </Col>
                                        </Row>
                                    </ListGroupItem >))}
                            </ListGroup>

                            <Row className="align-items-center mt-2 text-center text-sm-start">
                                <Col className="col-sm">
                                    <div className="text-muted">Showing<span className="fw-semibold">4</span> of <span className="fw-semibold">125</span> Results
                                    </div>
                                </Col>
                                <Col className="col-sm-auto">

                                    <Pagination size='sm' className="pagination-separated pagination-sm justify-content-center justify-content-sm-start mb-0">
                                        <PaginationItem> <PaginationLink to="#"> ← </PaginationLink> </PaginationItem>
                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                        <PaginationItem> <PaginationLink to="#"> → </PaginationLink> </PaginationItem>
                                    </Pagination>

                                </Col>
                            </Row>
                        </CardBody>
                    </Card>
                </Col>
                <Col xxl={7}>
                    <Row>
                        <Col xl={6}>
                            <Card className="card-height-100">
                                <CardHeader className="align-items-center d-flex">
                                    <h4 className="card-title mb-0 flex-grow-1">My Tasks</h4>
                                    <div className="flex-shrink-0">

                                        <UncontrolledDropdown className="card-header-dropdown">
                                            <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                                <span className="text-muted"><i className="ri-settings-4-line align-middle me-1 fs-15"></i>Settings</span>
                                            </DropdownToggle>
                                            <DropdownMenu className="dropdown-menu-end">
                                                <DropdownItem>Edit</DropdownItem>
                                                <DropdownItem>Remove</DropdownItem>
                                            </DropdownMenu>
                                        </UncontrolledDropdown>

                                    </div>
                                </CardHeader>

                                <CardBody className="p-0">

                                    <div className="align-items-center p-3 justify-content-between d-flex">
                                        <div className="flex-shrink-0">
                                            <div className="text-muted"><span className="fw-semibold">4</span> of <span className="fw-semibold">10</span> remaining</div>
                                        </div>
                                        <Button color="success" size="sm" ><i className="ri-add-line align-middle me-1"></i> Add Task</Button>
                                    </div>

                                    <SimpleBar style={{ maxHeight: "257px" }}>
                                        <ListGroup className="border-dashed px-3" flush>

                                            {(widgetsTasks || []).map((item, key) => (<ListGroupItem className="ps-0" key={key}>
                                                <div className="d-flex align-items-start">
                                                    <div className="form-check ps-0 flex-sharink-0">
                                                        <input type="checkbox" className="form-check-input ms-0" id={item.forId} />
                                                    </div>
                                                    <div className="flex-grow-1">
                                                        <label className="form-check-label mb-0 ps-2" htmlFor={item.forId}>{item.text}</label>
                                                    </div>
                                                    <div className="flex-shrink-0 ms-2">
                                                        <p className="text-muted fs-13 mb-0">{item.date}</p>
                                                    </div>
                                                </div>
                                            </ListGroupItem>))}

                                        </ListGroup>
                                    </SimpleBar>
                                    <div className="p-3">
                                        <Link to="#" className="text-muted text-decoration-underline">Show more...</Link>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xl={6}>
                            <Card className="card-height-100">
                                <CardHeader className="border-bottom-dashed align-items-center d-flex">
                                    <h4 className="card-title mb-0 flex-grow-1">Recent Activity</h4>
                                    <div className="flex-shrink-0">
                                        <button type="button" className="btn btn-soft-primary btn-sm">
                                            View All Activity
                                        </button>
                                    </div>
                                </CardHeader>
                                <CardBody className="p-0">
                                    <SimpleBar style={{ maxHeight: "364px" }} className="p-3">
                                        <div className="acitivity-timeline acitivity-main">
                                            <div className="acitivity-item d-flex">
                                                <div className="flex-shrink-0 avatar-xs acitivity-avatar">
                                                    <div className="avatar-title bg-soft-success text-success rounded-circle">
                                                        <i className="ri-shopping-cart-2-line"></i>
                                                    </div>
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h6 className="mb-1">Purchase by James Price</h6>
                                                    <p className="text-muted mb-1">Product noise evolve smartwatch </p>
                                                    <small className="mb-0 text-muted">02:14 PM Today</small>
                                                </div>
                                            </div>
                                            <div className="acitivity-item py-3 d-flex">
                                                <div className="flex-shrink-0 avatar-xs acitivity-avatar">
                                                    <div className="avatar-title bg-soft-primary text-primary rounded-circle">
                                                        <i className="ri-stack-fill"></i>
                                                    </div>
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h6 className="mb-1">Added new <span className="fw-semibold">style collection</span></h6>
                                                    <p className="text-muted mb-1">By Nesta Technologies</p>
                                                    <div className="d-inline-flex gap-2 border border-dashed p-2 mb-2 w-75">
                                                        <Link to="/apps-ecommerce-product-details" className="bg-light rounded p-1">
                                                            <img src={product8} alt="" className="img-fluid d-block" />
                                                        </Link>
                                                        <Link to="/apps-ecommerce-product-details" className="bg-light rounded p-1">
                                                            <img src={product2} alt="" className="img-fluid d-block" />
                                                        </Link>
                                                        <Link to="/apps-ecommerce-product-details" className="bg-light rounded p-1">
                                                            <img src={product10} alt="" className="img-fluid d-block" />
                                                        </Link>
                                                    </div>
                                                    <p className="mb-0 text-muted"><small>9:47 PM Yesterday</small></p>
                                                </div>
                                            </div>
                                            <div className="acitivity-item py-3 d-flex">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar2} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h6 className="mb-1">Natasha Carey have liked the products</h6>
                                                    <p className="text-muted mb-1">Allow users to like products in your WooCommerce store.</p>
                                                    <small className="mb-0 text-muted">25 Dec, 2021</small>
                                                </div>
                                            </div>
                                            <div className="acitivity-item py-3 d-flex">
                                                <div className="flex-shrink-0">
                                                    <div className="avatar-xs acitivity-avatar">
                                                        <div className="avatar-title rounded-circle bg-secondary">
                                                            <i className="mdi mdi-sale fs-14"></i>
                                                        </div>
                                                    </div>
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h6 className="mb-1">Today offers by <Link to="/apps-ecommerce-seller-details" className="link-secondary">Digitech Galaxy</Link></h6>
                                                    <p className="text-muted mb-2">Offer is valid on orders of Rs.500 Or above for selected products only.</p>
                                                    <small className="mb-0 text-muted">12 Dec, 2021</small>
                                                </div>
                                            </div>
                                            <div className="acitivity-item py-3 d-flex">
                                                <div className="flex-shrink-0">
                                                    <div className="avatar-xs acitivity-avatar">
                                                        <div className="avatar-title rounded-circle bg-soft-danger text-danger">
                                                            <i className="ri-bookmark-fill"></i>
                                                        </div>
                                                    </div>
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h6 className="mb-1">Favoried Product</h6>
                                                    <p className="text-muted mb-2">Esther James have favorited product.</p>
                                                    <small className="mb-0 text-muted">25 Nov, 2021</small>
                                                </div>
                                            </div>
                                            <div className="acitivity-item py-3 d-flex">
                                                <div className="flex-shrink-0">
                                                    <div className="avatar-xs acitivity-avatar">
                                                        <div className="avatar-title rounded-circle bg-secondary">
                                                            <i className="mdi mdi-sale fs-14"></i>
                                                        </div>
                                                    </div>
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h6 className="mb-1">Flash sale starting <span className="text-primary">Tomorrow.</span></h6>
                                                    <p className="text-muted mb-0">Flash sale by <Link to="#" className="link-secondary fw-medium">Zoetic Fashion</Link></p>
                                                    <small className="mb-0 text-muted">22 Oct, 2021</small>
                                                </div>
                                            </div>
                                            <div className="acitivity-item py-3 d-flex">
                                                <div className="flex-shrink-0">
                                                    <div className="avatar-xs acitivity-avatar">
                                                        <div className="avatar-title rounded-circle bg-soft-info text-info">
                                                            <i className="ri-line-chart-line"></i>
                                                        </div>
                                                    </div>
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h6 className="mb-1">Monthly sales report</h6>
                                                    <p className="text-muted mb-2"><span className="text-danger">2 days left</span> notification to submit the monthly sales report. <Link to="#" className="link-warning text-decoration-underline">Reports Builder</Link></p>
                                                    <small className="mb-0 text-muted">15 Oct</small>
                                                </div>
                                            </div>
                                            <div className="acitivity-item d-flex">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar3} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <h6 className="mb-1">Frank Hook Commented</h6>
                                                    <p className="text-muted mb-2 fst-italic">" A product that has reviews is more likable to be sold than a product. "</p>
                                                    <small className="mb-0 text-muted">26 Aug, 2021</small>
                                                </div>
                                            </div>
                                        </div>
                                    </SimpleBar>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Col>
            </Row>
            <Row>
                {(chartDataOptions || []).map((item, key) => (
                    <Col xl={4} md={6} key={key}>
                        <Card className="card-height-100">
                            <div className="d-flex">
                                <div className="flex-grow-1 p-3">
                                    <h5 className="mb-3">{item.title}</h5>
                                    <p className="mb-0 text-muted"><span className="badge bg-light text-success mb-0"> <i className="ri-arrow-up-line align-middle"></i> {item.persantage} % </span> vs. previous month</p>
                                </div>
                                <div>
                                    {/* <div className="apex-charts" data-colors='["--vz-success" , "--vz-transparent"]' dir="ltr" id="results_sparkline_charts"></div> */}
                                    <OtherWidgetsCharts dataColors={item.dataColors} seriesData={item.series} />
                                </div>
                            </div>
                        </Card>
                    </Col>
                ))}
            </Row>
        </React.Fragment>
    );
};

export default UpcomingActivity;