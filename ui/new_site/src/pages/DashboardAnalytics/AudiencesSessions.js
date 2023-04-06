import React, { useState, useEffect } from 'react';
import { Card, CardHeader, Col, DropdownMenu, DropdownToggle, UncontrolledDropdown, DropdownItem } from 'reactstrap';
import { useSelector, useDispatch } from "react-redux";
import { AudiencesSessionsCharts } from './DashboardAnalyticsCharts';
import { getAudiencesSessionsChartsData } from "../../slices/thunks";

const AudiencesSessions = () => {
    const dispatch = useDispatch();

    const [chartData, setchartData] = useState([]);

    const { audiencesSessionsData } = useSelector(state => ({
        audiencesSessionsData: state.DashboardAnalytics.audiencesSessionsData
    }));

    useEffect(() => {
        setchartData(audiencesSessionsData);
    }, [audiencesSessionsData]);

    const [seletedMonth, setSeletedMonth] = useState("today");
    const onChangeChartPeriod = pType => {
        setSeletedMonth(pType);
        dispatch(getAudiencesSessionsChartsData(pType));
    };

    useEffect(() => {
        dispatch(getAudiencesSessionsChartsData("today"));
    }, [dispatch]);
    return (
        <React.Fragment>
            <Col xl={6}>
                <Card className="card-height-100">
                    <CardHeader className="align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Audiences Sessions by Country</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown">
                                <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                    <span className="fw-semibold text-uppercase fs-13">Sort by: </span><span className="text-muted">{seletedMonth.charAt(0).toUpperCase() + seletedMonth.slice(1)}<i className="mdi mdi-chevron-down ms-1"></i></span>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu-end">
                                    <DropdownItem onClick={() => { onChangeChartPeriod("today"); }} className={seletedMonth === "today" ? "active" : ""}>Today</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("lastWeek"); }} className={seletedMonth === "lastWeek" ? "active" : ""}>Last Week</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("lastMonth"); }} className={seletedMonth === "lastMonth" ? "active" : ""}>Last Month</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("currentYear"); }} className={seletedMonth === "currentYear" ? "active" : ""}>Current Year</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </CardHeader>
                    <div className="card-body p-0">
                        <div>
                            <div id="audiences-sessions-country-charts" className="apex-charts" dir="ltr">
                                <AudiencesSessionsCharts series={chartData} dataColors='["--vz-success", "--vz-secondary"]' />
                            </div>
                        </div>
                    </div>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default AudiencesSessions;