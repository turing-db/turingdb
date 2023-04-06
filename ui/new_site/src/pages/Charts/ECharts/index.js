import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import {LineChart,StackedLineChart,AreaChart,StackedAreaChart,StepLineChart,LineYChart,BasicBarChart,
        BarLabelChart,HorizontalBarChart,StackedHorizontalBarChart,PieChart,DoughnutChart,BasicScatterChart,
        CandleStickChart,GraphChart,TreemapChart,SunburstChart,ParallelChart,SankeyChart,FunnelChart,GaugeChart,HeatmapChart
        } from './ECharts'

const Echarts = () => {
  document.title="E Charts | Velzon - React Admin & Dashboard Template";
    return (
        <div className="page-content">
            <Container fluid>                
                <BreadCrumb title="E Charts" pageTitle="Charts" />
                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Line Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <LineChart dataColors='["--vz-success"]'/>
                            </CardBody>
                        </Card>
                    </Col>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Stacked Line Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <StackedLineChart dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>
                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Area Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <AreaChart dataColors='["--vz-success"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Stacked Area Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <StackedAreaChart dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Step Line</h4>
                            </CardHeader>
                            <div className="card-body">
                                <StepLineChart dataColors='["--vz-primary", "--vz-success", "--vz-warning"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Line Y Category</h4>
                            </CardHeader>
                            <div className="card-body">
                                <LineYChart dataColors='["--vz-success"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Basic Bar</h4>
                            </CardHeader>
                            <div className="card-body">
                                <BasicBarChart dataColors='["--vz-success"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Bar Label</h4>
                            </CardHeader>
                            <div className="card-body">
                                <BarLabelChart dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>


                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Horizontal Bar</h4>
                            </CardHeader>
                            <div className="card-body">
                                <HorizontalBarChart dataColors='["--vz-primary", "--vz-success"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Stacked Horizontal Bar</h4>
                            </CardHeader>
                            <div className="card-body">
                                <StackedHorizontalBarChart dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Pie Charts</h4>
                            </CardHeader>
                            <div className="card-body">
                                <PieChart dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Doughnut Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <DoughnutChart dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Basic Scatter Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <BasicScatterChart dataColors='["--vz-primary"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Candlestick Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <CandleStickChart dataColors='["--vz-danger", "--vz-success"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Graph Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <GraphChart dataColors='["--vz-primary"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Treemap Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <TreemapChart dataColors='["--vz-primary", "--vz-success"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Sunburst Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <SunburstChart dataColors='["--vz-warning", "--vz-success"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Parallel Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <ParallelChart dataColors='["--vz-success"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Sankey Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <SankeyChart dataColors='["--vz-info", "--vz-success", "--vz-warning", "--vz-danger", "--vz-primary"]'/>
                            </div>
                        </Card>
                    </Col>


                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Funnel Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <FunnelChart dataColors='["--vz-info", "--vz-success", "--vz-warning", "--vz-danger", "--vz-primary"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>


                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Gauge Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <GaugeChart dataColors='["--vz-primary"]'/>
                            </div>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Heatmap Chart</h4>
                            </CardHeader>
                            <div className="card-body">
                                <HeatmapChart dataColors='["--vz-primary", "--vz-warning"]'/>
                            </div>
                        </Card>
                    </Col>
                </Row>
            </Container>

        </div>
    )
}

export default Echarts