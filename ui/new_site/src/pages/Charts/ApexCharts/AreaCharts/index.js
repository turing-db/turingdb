import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';
import user from "../../../../assets/images/users/avatar-2.jpg";

import {
    BasicAreaCharts,
    SplineAreaChart,
    AxisChart,
    NegativeAreaChart,
    GithubStyleCharts,
    GithubStyleCharts1,
    StackedAreaChart,
    IrregularAreaCharts,
    AreaNullValueChart
} from "./AreaCharts";
import { Link } from 'react-router-dom';

const AreaCharts = () => {
    document.title="Area Charts | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Area Charts" pageTitle="Apexcharts" />
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Basic Area Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <BasicAreaCharts dataColors='["--vz-success"]' />
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Spline Area Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <SplineAreaChart dataColors='["--vz-primary", "--vz-success"]' />
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Area Chart - Datetime X - Axis Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <div>
                                        <div className="toolbar d-flex align-items-start justify-content-center flex-wrap gap-2">
                                            <button type="button" className="btn btn-soft-primary timeline-btn btn-sm" id="one_month">
                                                1M
                                            </button>
                                            <button type="button" className="btn btn-soft-primary timeline-btn btn-sm" id="six_months">
                                                6M
                                            </button>
                                            <button type="button" className="btn btn-soft-primary timeline-btn btn-sm active" id="one_year">
                                                1Y
                                            </button>
                                            <button type="button" className="btn btn-soft-primary timeline-btn btn-sm" id="all">
                                                ALL
                                            </button>
                                        </div>
                                        <AxisChart dataColors='["--vz-info"]'/>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Area with Negative Values Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <NegativeAreaChart dataColors='["--vz-success", "--vz-info"]'/>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Area Chart - Github Style</h4>
                                </CardHeader>
                                <CardBody>
                                    <div>
                                        <div className="bg-light">
                                            <GithubStyleCharts dataColors='["--vz-danger"]'/>
                                        </div>

                                        <div className="github-style d-flex align-items-center my-2">
                                            <div className="flex-shrink-0 me-2">
                                                <img className="avatar-sm rounded" src={user} data-hovercard-user-id="634573" alt="" />
                                            </div>
                                            <div className="flex-grow-1">
                                                <Link to="#" className="font-size-14 text-dark fw-medium">coder</Link>
                                                <div className="cmeta text-muted font-size-11">
                                                    <span className="commits text-dark fw-medium"></span> commits
                                                </div>
                                            </div>
                                        </div>

                                        <div className="bg-light">
                                            <GithubStyleCharts1 dataColors='["--vz-success"]'/>
                                        </div>

                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Stacked Area Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <StackedAreaChart dataColors='["--vz-success", "--vz-info", "--vz-light"]'/>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Irregular Timeseries Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <IrregularAreaCharts dataColors='["--vz-primary", "--vz-warning", "--vz-success"]'/>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Area Chart With Null Values Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <AreaNullValueChart dataColors='["--vz-success"]'/>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                </Container>
            </div>
        </React.Fragment>
    );
};

export default AreaCharts;