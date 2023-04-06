import React from 'react';
import { Link } from 'react-router-dom';
import { Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledDropdown } from 'reactstrap';
import { activities } from "../../common/data";

const UpcomingActivities = () => {
    return (
        <React.Fragment>
            <Col xxl={5}>
                <Card>
                    <CardHeader className="align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Upcoming Activities</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown" direction="start">
                                <DropdownToggle className="text-reset dropdown-btn" tag="a" role="button">
                                    <span className="text-muted fs-18"><i className="mdi mdi-dots-vertical"></i></span>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu dropdown-menu-end">
                                    <DropdownItem>Edit</DropdownItem>
                                    <DropdownItem>Remove</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </CardHeader>
                    <CardBody className="card-body pt-0">
                        <ul className="list-group list-group-flush border-dashed">
                            {(activities || []).map((item, key) => (
                                <li className="list-group-item ps-0" key={key}>
                                    <Row className="align-items-center g-3">
                                        <div className="col-auto">
                                            <div className="avatar-sm p-1 py-2 h-auto bg-light rounded-3">
                                                <div className="text-center">
                                                    <h5 className="mb-0">{item.date}</h5>
                                                    <div className="text-muted">{item.weekDay}</div>
                                                </div>
                                            </div>
                                        </div>
                                        <div className="col">
                                            <h5 className="text-muted mt-0 mb-1 fs-13">{item.time}</h5>
                                            <Link to="#" className="text-reset fs-14 mb-0">{item.caption}</Link>
                                        </div>
                                        <div className="col-sm-auto">
                                            <div className="avatar-group">
                                                {(item.subItem || []).map((subItem, key) => (
                                                    <React.Fragment key={key}>
                                                        <div className="avatar-group-item">
                                                            <Link to="#" className="d-inline-block" data-bs-toggle="tooltip" data-bs-placement="top" title="" data-bs-original-title="Stine Nielsen">
                                                                <img src={subItem.img} alt="" className="rounded-circle avatar-xxs" />
                                                            </Link>
                                                        </div>
                                                    </React.Fragment>
                                                ))}
                                                <div className="avatar-group-item">
                                                    <Link to="#">
                                                        <div className="avatar-xxs">
                                                            <span className={"avatar-title rounded-circle " + item.bgcolor + " text-white"}>
                                                                {item.imgNumber}
                                                            </span>
                                                        </div>
                                                    </Link>
                                                </div>
                                            </div>
                                        </div>
                                    </Row>
                                </li>
                            ))}
                        </ul>
                        <div className="align-items-center mt-2 row g-3 text-center text-sm-start">
                            <div className="col-sm">
                                <div className="text-muted">Showing <span className="fw-semibold"> 4</span> of <span className="fw-semibold">125</span> Results
                                </div>
                            </div>
                            <div className="col-sm-auto">
                                <ul className="pagination pagination-separated pagination-sm justify-content-center justify-content-sm-start mb-0">
                                    <li className="page-item disabled">
                                        <Link to="#" className="page-link">←</Link>
                                    </li>
                                    <li className="page-item">
                                        <Link to="#" className="page-link">1</Link>
                                    </li>
                                    <li className="page-item active">
                                        <Link to="#" className="page-link">2</Link>
                                    </li>
                                    <li className="page-item">
                                        <Link to="#" className="page-link">3</Link>
                                    </li>
                                    <li className="page-item">
                                        <Link to="#" className="page-link">→</Link>
                                    </li>
                                </ul>
                            </div>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default UpcomingActivities;