import React, { useState, useEffect } from 'react';
import { Card, CardBody, CardHeader, Col, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';
import { useSelector, useDispatch } from "react-redux";
import { PrjectsStatusCharts } from './DashboardProjectCharts';
import { getProjectStatusChartsData } from '../../slices/thunks';

const ProjectsStatus = () => {
    const dispatch = useDispatch();

    const [chartData, setchartData] = useState([]);

    const { projectStatusData } = useSelector(state => ({
        projectStatusData: state.DashboardProject.projectStatusData
    }));

    useEffect(() => {
        setchartData(projectStatusData);
    }, [projectStatusData]);

    const [seletedMonth, setSeletedMonth] = useState("All Time");
    const onChangeChartPeriod = pType => {
        setSeletedMonth(pType);
        dispatch(getProjectStatusChartsData(pType));
    };

    useEffect(() => {
        dispatch(getProjectStatusChartsData("all"));
    }, [dispatch]);
    return (
        <React.Fragment>
            <Col xxl={4} lg={6}>
                <Card className="card-height-100">
                    <CardHeader className="align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Projects Status</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown">
                                <DropdownToggle tag="a" className="dropdown-btn text-muted" role="button">
                                {seletedMonth.charAt(0).toUpperCase() + seletedMonth.slice(1)} <i className="mdi mdi-chevron-down ms-1"></i>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu-end">
                                    <DropdownItem onClick={() => { onChangeChartPeriod("all"); }} className={seletedMonth === "all" ? "active" : ""}>All Time</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("week"); }} className={seletedMonth === "week" ? "active" : ""}>Last 7 Days</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("month"); }} className={seletedMonth === "month" ? "active" : ""}>Last 30 Days</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("quarter"); }} className={seletedMonth === "quarter" ? "active" : ""}>Last 90 Days</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </CardHeader>

                    <CardBody>
                        <div id="prjects-status" className="apex-charts" dir="ltr">
                            <PrjectsStatusCharts series={chartData} dataColors='["--vz-success", "--vz-primary", "--vz-warning", "--vz-danger"]' />
                        </div>
                        <div className="mt-3">
                            <div className="d-flex justify-content-center align-items-center mb-4">
                                <h2 className="me-3 ff-secondary mb-0">{chartData[0] + chartData[1] + chartData[2] + chartData[3] || 784}</h2>
                                <div>
                                    <p className="text-muted mb-0">Total Projects</p>
                                    <p className="text-success fw-medium mb-0">
                                        <span className="badge badge-soft-success p-1 rounded-circle"><i className="ri-arrow-right-up-line"></i></span> +3 New
                                    </p>
                                </div>
                            </div>

                            <div className="d-flex justify-content-between border-bottom border-bottom-dashed py-2">
                                <p className="fw-medium mb-0"><i className="ri-checkbox-blank-circle-fill text-success align-middle me-2"></i> Completed</p>
                                <div>
                                    <span className="text-muted pe-5">{chartData[0]}Projects</span>
                                    <span className="text-success fw-medium fs-13">15870hrs</span>
                                </div>
                            </div>
                            <div className="d-flex justify-content-between border-bottom border-bottom-dashed py-2">
                                <p className="fw-medium mb-0"><i className="ri-checkbox-blank-circle-fill text-primary align-middle me-2"></i> In Progress</p>
                                <div>
                                    <span className="text-muted pe-5">{chartData[1]}Projects</span>
                                    <span className="text-success fw-medium fs-13">243hrs</span>
                                </div>
                            </div>
                            <div className="d-flex justify-content-between border-bottom border-bottom-dashed py-2">
                                <p className="fw-medium mb-0"><i className="ri-checkbox-blank-circle-fill text-warning align-middle me-2"></i> Yet to Start</p>
                                <div>
                                    <span className="text-muted pe-5">{chartData[2]}Projects</span>
                                    <span className="text-success fw-medium fs-13">~2050hrs</span>
                                </div>
                            </div>
                            <div className="d-flex justify-content-between py-2">
                                <p className="fw-medium mb-0"><i className="ri-checkbox-blank-circle-fill text-danger align-middle me-2"></i> Cancelled</p>
                                <div>
                                    <span className="text-muted pe-5">{chartData[3]}Projects</span>
                                    <span className="text-success fw-medium fs-13">~900hrs</span>
                                </div>
                            </div>
                        </div>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default ProjectsStatus;