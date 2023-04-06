import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';

import {
    Basic,
    ComboCandlestick,
    ComboCandlestick1,
    Category,
    CandlestickLine
} from "./CandlestickChart";

const CandlestickChart = () => {
    document.title="Candlestick Charts | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">               
                <Container fluid>
                    <BreadCrumb title="Candlestick Charts" pageTitle="Apexcharts" />
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Basic Candlestick Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <Basic dataColors='["--vz-success", "--vz-danger"]'/>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Candlestick Synced with Brush Chart (Combo)</h4>
                                </CardHeader>
                                <CardBody>
                                    <div>
                                        <div id="combo_candlestick">
                                            <ComboCandlestick dataColors='["--vz-info", "--vz-danger"]'/>
                                        </div>
                                        <div id="combo_candlestick_chart">
                                            <ComboCandlestick1 dataColors='["--vz-warning", "--vz-danger"]'/>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Category X-Axis</h4>
                                </CardHeader>
                                <CardBody>
                                    <Category dataColors='["--vz-success", "--vz-danger"]'/>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xl={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Candlestick with line</h4>
                                </CardHeader>

                                <CardBody>
                                    <CandlestickLine dataColors='["--vz-success", "--vz-danger", "--vz-info", "--vz-warning"]'/>
                                </CardBody>
                            </Card>
                        </Col>
                        </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default CandlestickChart;