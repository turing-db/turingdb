import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';
import {BasicPolar,PolarMonochrome} from './PolarCharts'

const Polarcharts = () => {
    document.title="Polar Charts | Velzon - React Admin & Dashboard Template";
    return (
        <div className="page-content">
            <Container fluid>                
                <BreadCrumb title="Polar Charts" pageTitle="Apexcharts" />
                <Row>
                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">Basic Polararea Chart</h4>
                            </CardHeader>
                            <CardBody>
                                <BasicPolar dataColors='["--vz-primary", "--vz-success", "--vz-warning","--vz-danger", "--vz-info", "--vz-success", "--vz-primary", "--vz-dark", "--vz-secondary"]'/>
                            </CardBody>
                        </Card>
                    </Col>

                    <Col xl={6}>
                        <Card>
                            <CardHeader>
                                <h4 className="card-title mb-0">PolarArea Monochrome</h4>
                            </CardHeader>
                            <CardBody>
                                <PolarMonochrome />
                            </CardBody>
                        </Card>
                    </Col>
                </Row>
            </Container>
        </div>
    )
}

export default Polarcharts