import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';

import {
    Basic,
    Datetime,
    ImagesChart
} from "./ScatterCharts";

const ScatterCharts = () => {
    document.title="Scatter Charts | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Scatter Charts" pageTitle="Apexcharts" />
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Basic Scatter Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <Basic dataColors='["--vz-primary", "--vz-success", "--vz-warning"]'/>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Scatter - Datetime Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <Datetime dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-warning", "--vz-info"]'/>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>        
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Scatter Images Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <ImagesChart dataColors='["--vz-primary", "--vz-danger"]'/>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default ScatterCharts;