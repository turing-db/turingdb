import React, { useState, useEffect } from 'react';
import { Card, Col, DropdownItem, DropdownMenu, DropdownToggle, UncontrolledDropdown } from 'reactstrap';
import { useSelector, useDispatch } from "react-redux";
import { DealTypeCharts } from "./DashboardCrmCharts";
import { getDialChartsData } from '../../slices/thunks';

const DealType = () => {
    const dispatch = useDispatch();

    const [chartData, setchartData] = useState([]);

    const { dialTypeData } = useSelector(state => ({
        dialTypeData: state.DashboardCRM.dialTypeData
    }));

    useEffect(() => {
        setchartData(dialTypeData);
    }, [dialTypeData]);

    const [seletedMonth, setSeletedMonth] = useState("Monthly");
    const onChangeChartPeriod = pType => {
        setSeletedMonth(pType);
        dispatch(getDialChartsData(pType));
    };

    useEffect(() => {
        dispatch(getDialChartsData("monthly"));
    }, [dispatch]);

    return (
        <React.Fragment>
            <Col xxl={3} md={6}>
                <Card className="card-height-100">
                    <div className="card-header align-items-center d-flex">
                        <h4 className="card-title mb-0 flex-grow-1">Deal Type</h4>
                        <div className="flex-shrink-0">
                            <UncontrolledDropdown className="card-header-dropdown">
                                <DropdownToggle tag="a" className="text-reset dropdown-btn" role="button">
                                    <span className="fw-semibold text-uppercase fs-12">Sort by: </span><span className="text-muted">{seletedMonth.charAt(0).toUpperCase() + seletedMonth.slice(1)}<i className="mdi mdi-chevron-down ms-1"></i></span>
                                </DropdownToggle>
                                <DropdownMenu className="dropdown-menu-end">
                                    <DropdownItem onClick={() => { onChangeChartPeriod("today"); }} className={seletedMonth === "today" ? "active" : ""}>Today</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("weekly"); }} className={seletedMonth === "weekly" ? "active" : ""}>Weekly</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("monthly"); }} className={seletedMonth === "monthly" ? "active" : ""}>Monthly</DropdownItem>
                                    <DropdownItem onClick={() => { onChangeChartPeriod("yearly"); }} className={seletedMonth === "yearly" ? "active" : ""}>Yearly</DropdownItem>
                                </DropdownMenu>
                            </UncontrolledDropdown>
                        </div>
                    </div>
                    <div className="card-body pb-0">
                        <div id="deal-type-charts" dir="ltr">
                            <DealTypeCharts series={chartData} dataColors='["--vz-warning", "--vz-secondary", "--vz-success"]' />
                        </div>
                    </div>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default DealType;