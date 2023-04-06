import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';
import {
    Simple,
    ThreeDBubble
} from './BubbleChart';

const BubbleChart = () => {
    document.title="Bubble Charts | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment> 
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Bubble Charts" pageTitle="Apexcharts" />
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Basic Candlestick Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <Simple dataColors='["--vz-primary", "--vz-info", "--vz-warning", "--vz-success"]'/>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">3D Bubble Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <ThreeDBubble dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger"]'/>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default BubbleChart;