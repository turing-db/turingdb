import React from 'react';
import { Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';
import { Link } from 'react-router-dom';
import { dealsStatus } from "../../common/data";

const DealsStatus = () => {
    return (
        <React.Fragment>
            <Col xl={7}>
                <Card>
                    <CardHeader className="align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Deals Status</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown">
                                <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                    <span className="text-muted">02 Nov 2021 to 31 Dec 2021<i className="mdi mdi-chevron-down ms-1"></i></span>
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

                    <CardBody>
                        <div className="table-responsive table-card">
                            <table className="table table-borderless table-hover table-nowrap align-middle mb-0">
                                <thead className="table-light">
                                    <tr className="text-muted">
                                        <th scope="col">Name</th>
                                        <th scope="col" style={{ width: "20%" }}>Last Contacted</th>
                                        <th scope="col" >Sales Representative</th>
                                        <th scope="col" style={{ width: "16%" }}>Status</th>
                                        <th scope="col" style={{ width: "12%" }}>Deal Value</th>
                                    </tr>
                                </thead>

                                <tbody>
                                    {(dealsStatus || []).map((item, index) => (
                                        <tr key={index}>
                                            <td>{item.name}</td>
                                            <td>{item.date}</td>
                                            <td><img src={item.img} alt="" className="avatar-xs rounded-circle me-2" />
                                                <Link to="#" className="text-body fw-medium">{item.representativeName}</Link></td>
                                            <td><span className={"badge badge-soft-" + item.badgeClass + " p-2"}>{item.status}</span></td>
                                            <td><div className="text-nowrap">{item.statusValue}</div></td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default DealsStatus;