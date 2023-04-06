import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';

import {
    BasicHeatmap,
    MultipleHeatmap,
    ColorRange,
    RangeWithoutShades
} from "./HeatmapCharts";


const HeatmapCharts = () => {
    document.title="Heatmap Charts | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>                           
                    <BreadCrumb title="Heatmap Charts" pageTitle="Apexcharts" />
                    <Row>
                        <Col xl={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Basic Heatmap Chart</h4>
                                </CardHeader>
                                <CardBody>
                                    <div className="live-preview">
                                        <BasicHeatmap dataColors='["--vz-success", "--vz-card-custom"]'/>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={6}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Heatmap - Multiple Series</h4>
                                </CardHeader>

                                <CardBody>
                                    <MultipleHeatmap dataColors='["--vz-primary", "--vz-secondary", "--vz-success", "--vz-info", "--vz-warning", "--vz-danger", "--vz-dark", "--vz-primary"]'/>
                                </CardBody>
                            </Card>
                        </Col>

                    </Row>

                    <Row>
                        <Col xl={6}>
                            <Card>
                                <div className="card-header">
                                    <h4 className="card-title mb-0">Heatmap Color Range</h4>
                                </div>

                                <div className="card-body">
                                    <ColorRange dataColors='["--vz-info", "--vz-success", "--vz-primary", "--vz-warning"]'/>
                                </div>
                            </Card>
                        </Col>

                        <Col xl={6}>
                            <Card>
                                <div className="card-header">
                                    <h4 className="card-title mb-0">Heatmap - Range Without Shades</h4>
                                </div>

                                <div className="card-body">
                                <RangeWithoutShades dataColors='["--vz-info", "--vz-primary"]'/>
                                </div>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default HeatmapCharts;