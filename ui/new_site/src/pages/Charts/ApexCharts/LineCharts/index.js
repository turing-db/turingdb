import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';

import {
    BasicLineCharts,
    ZoomableTimeseries,
    LinewithDataLabels,
    DashedLine,
    LinewithAnnotations,
    BrushChart,
    BrushChart1,
    SteplineChart,
    GradientCharts,
    MissingData,
    ChartSyncingLine,
    ChartSyncingLine2,
    ChartSyncingArea
} from "./LineCharts";


const LineCharts = () => {
    document.title="Line Charts | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Line Charts" pageTitle="Apexcharts" />
                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Basic Line Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <div>
                                        <BasicLineCharts dataColors='["--vz-primary"]' />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Zoomable Timeseries</h4>
                                </CardHeader>
                                <CardBody>
                                    <ZoomableTimeseries dataColors='["--vz-success"]' />
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Line with Data Labels</h4>
                                </CardHeader>
                                <CardBody>
                                    <LinewithDataLabels dataColors='["--vz-primary", "--vz-success"]' />
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Dashed Line</h4>
                                </CardHeader>
                                <CardBody>
                                    <DashedLine dataColors='["--vz-primary", "--vz-danger", "--vz-success"]' />
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Line with Annotations</h4>
                                </CardHeader>
                                <CardBody>
                                    <LinewithAnnotations dataColors='["--vz-primary"]' />
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Brush Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <div>
                                        <BrushChart dataColors='["--vz-danger"]' />
                                    </div>
                                    <div>
                                        <BrushChart1 dataColors='["--vz-info"]' />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Stepline Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <SteplineChart dataColors='["--vz-success"]' />
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Gradient Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <div>
                                        <GradientCharts dataColors='["--vz-success"]' />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Missing Data/ Null Value Charts</h4>
                                </CardHeader>
                                <CardBody>
                                    <MissingData dataColors='["--vz-primary", "--vz-danger", "--vz-success"]' />
                                </CardBody>
                            </Card>
                        </Col>
                        <Col lg={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Syncing Charts</h4>
                                </CardHeader>
                                <CardBody>
                                    <div>
                                        <ChartSyncingLine dataColors='["--vz-primary"]' className="apex-charts" dir="ltr"/>
                                        <ChartSyncingLine2 dataColors='["--vz-warning"]' className="apex-charts" dir="ltr"/>
                                        <ChartSyncingArea dataColors='["--vz-success"]' className="apex-charts" dir="ltr"/>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default LineCharts;