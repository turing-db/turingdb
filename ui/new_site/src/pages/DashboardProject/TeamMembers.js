import React from 'react';
import { Card, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';
import { teamMembers } from '../../common/data';
import { TeamMembersCharts } from './DashboardProjectCharts';

const TeamMembers = () => {
    return (
        <React.Fragment>
            <Col xxl={4}>
                <Card className='card-height-100'>
                    <CardHeader className="align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Team Members</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown">
                                <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                    <span className="fw-semibold text-uppercase fs-12">Sort by: </span><span className="text-muted">Last 30 Days<i className="mdi mdi-chevron-down ms-1"></i></span>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu dropdown-menu-end">
                                    <DropdownItem>Today</DropdownItem>
                                    <DropdownItem>Yesterday</DropdownItem>
                                    <DropdownItem>Last 7 Days</DropdownItem>
                                    <DropdownItem>Last 30 Days</DropdownItem>
                                    <DropdownItem>This Month</DropdownItem>
                                    <DropdownItem>Last Month</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </CardHeader>

                    <div className="card-body">

                        <div className="table-responsive table-card">
                            <table className="table table-borderless table-nowrap align-middle mb-0">
                                <thead className="table-light text-muted">
                                    <tr>
                                        <th scope="col">Member</th>
                                        <th scope="col">Hours</th>
                                        <th scope="col">Tasks</th>
                                        <th scope="col">Status</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {(teamMembers || []).map((item, key) => (<tr key={key}>
                                        <td className="d-flex">
                                            <img src={item.img} alt="" className="avatar-xs rounded-3 me-2" />
                                            <div>
                                                <h6 className="mb-0">{item.name}</h6>
                                                <p className="fs-13 mb-0 text-muted">{item.position}</p>
                                            </div>
                                        </td>
                                        <td>
                                            <h6 className="mb-0">{item.hours}h : <span className="text-muted"> 150h </span></h6>
                                        </td>
                                        <td>
                                            {item.tasks}
                                        </td>
                                        <td style={{ width: "5%" }}>
                                            
                                            {/* <div id="radialBar_chart_1" data-colors='["--vz-primary"]' data-chart-series="50" className="apex-charts" dir="ltr"> */}
                                                <TeamMembersCharts seriesData={item.series} chartsColor={item.chartsColor} />
                                            {/* </div> */}
                                        </td>
                                    </tr>))}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </Card>
            </Col>

        </React.Fragment>
    );
};

export default TeamMembers;