import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';
import { SimpleRadialbar, MultipleRadialbar, CircleRadialbar, GradientCircleRadialbar, StrokedCircleRadial, SemiCircularRadial, ImageRadialbar } from './RadialbarCharts';

const RadialbarCharts = () => {
    document.title = "Radialbar Charts | Velzon - React Admin & Dashboard Template";

    return (
        <div className="page-content">
            <Container fluid>
                <BreadCrumb title="Radialbar Charts" pageTitle="Apexcharts" />
                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Simple Radialbar Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <SimpleRadialbar dataColors='["--vz-success"]' />
                            </CardBody>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Multiple Radialbar</h4>
                            </CardHeader>
                            <CardBody>
                                <MultipleRadialbar dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger"]' />
                            </CardBody>
                        </Card>
                    </Col>
                </Row>
                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Circle Chart - Custom Angle</h4>
                            </CardHeader>
                            <CardBody>
                                <CircleRadialbar dataColors='["--vz-primary", "--vz-info", "--vz-danger", "--vz-success"]' />
                            </CardBody>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Gradient Circle Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <GradientCircleRadialbar dataColors='["--vz-success"]' />
                            </CardBody>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Stroked Circle Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <StrokedCircleRadial dataColors='["--vz-success"]' />
                            </CardBody>
                        </Card>
                    </Col>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Radialbars with Image</h4>
                            </CardHeader>

                            <CardBody>
                                <ImageRadialbar dataColors='["--vz-success"]' />
                            </CardBody>
                        </Card>
                    </Col>
                </Row>
                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Semi Circular Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <SemiCircularRadial dataColors='["--vz-primary"]' />
                            </CardBody>
                        </Card>
                    </Col>
                </Row>
            </Container>
        </div>
    );
};

export default RadialbarCharts;