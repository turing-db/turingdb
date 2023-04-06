import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import { LineChart ,BarChart,PieChart,DonutChart,PolarChart,RadarChart} from './ChartsJs';


const ChartsJs = () => {
  document.title="Charts Js | Velzon - React Admin & Dashboard Template";

    return (
        <div className="page-content">
            <Container fluid>                
                <BreadCrumb title="Charts Js" pageTitle="Charts" />
                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Line Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <LineChart dataColors='["--vz-primary-rgb, 0.2", "--vz-primary", "--vz-success-rgb, 0.2", "--vz-success"]'/>
                            </CardBody>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Bar Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <BarChart dataColors='["--vz-primary-rgb, 0.8", "--vz-primary-rgb, 0.9"]'/>
                            </CardBody>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Pie Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <PieChart dataColors='["--vz-success", "--vz-light"]'/>
                            </CardBody>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Donut Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <DonutChart dataColors='["--vz-primary", "--vz-light"]'/>
                            </CardBody>
                        </Card>
                    </Col>
                </Row>

                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Polar Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <PolarChart dataColors='["--vz-danger", "--vz-success", "--vz-warning", "--vz-primary"]' />
                            </CardBody>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Radar Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <RadarChart dataColors='["--vz-success-rgb, 0.2", "--vz-success", "--vz-primary-rgb, 0.2", "--vz-primary"]'/>
                            </CardBody>
                        </Card>
                    </Col>
                </Row>

            </Container>

        </div>
    )
}

export default ChartsJs