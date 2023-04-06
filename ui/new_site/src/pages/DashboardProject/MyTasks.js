import React from 'react';
import { Link } from 'react-router-dom';
import { Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';
import { projectTasks } from '../../common/data';

const MyTasks = () => {
    return (
        <React.Fragment>
            <Col xl={5}>
                <Card>
                    <CardHeader className="card-header align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1 py-1">My Tasks</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown">
                                <DropdownToggle className="text-reset dropdown-btn" tag="a" role="button">
                                    <span className="text-muted">All Tasks <i className="mdi mdi-chevron-down ms-1"></i></span>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu-end">
                                    <DropdownItem>All Tasks</DropdownItem>
                                    <DropdownItem>Completed </DropdownItem>
                                    <DropdownItem>Inprogress</DropdownItem>
                                    <DropdownItem>Pending</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </CardHeader>
                    <CardBody>
                        <div className="table-responsive table-card">
                            <table className="table table-borderless table-nowrap table-centered align-middle mb-0">
                                <thead className="table-light text-muted">
                                    <tr>
                                        <th scope="col">Name</th>
                                        <th scope="col">Dedline</th>
                                        <th scope="col">Status</th>
                                        <th scope="col">Assignee</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {(projectTasks || []).map((item, key) => (<tr key={key}>
                                        <td>
                                            <div className="form-check">
                                                <input className="form-check-input fs-15" type="checkbox" value="" id={item.forId} />
                                                <label className="form-check-label ms-1" htmlFor={item.forId}>
                                                    {item.label}
                                                </label>
                                            </div>
                                        </td>
                                        <td className="text-muted">{item.dedline}</td>
                                        <td><span className={"badge badge-soft-" + item.statusClass}>{item.status}</span></td>
                                        <td>
                                            <Link to="#" className="d-inline-block" data-bs-toggle="tooltip" data-bs-placement="top" title="" data-bs-original-title="Mary Stoner">
                                                <img src={item.img} alt="" className="rounded-circle avatar-xxs" />
                                            </Link>
                                        </td>
                                    </tr>))}
                                </tbody>
                            </table>
                        </div>
                        <div className="mt-3 text-center">
                            <Link to="#" className="text-muted text-decoration-underline">Load More</Link>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default MyTasks;