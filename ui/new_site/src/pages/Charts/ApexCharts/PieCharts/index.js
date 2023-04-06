import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../../Components/Common/BreadCrumb';
import {SimplePie,SimpleDonut,UpdateDonut,MonochromePie,GradientDonut,PatternedDonut,ImagePieChart} from './PieCharts'

const PieCharts = () => {
  document.title="Pie Charts | Velzon - React Admin & Dashboard Template";
  return (
    <div className="page-content">
                <Container fluid>                              
                    <BreadCrumb title="Pie Charts" pageTitle="Apexcharts" />
                        <Row>
                            <Col xl={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Simple Pie Chart</h4>
                                    </CardHeader>

                                    <CardBody>
                                        <SimplePie dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                                    </CardBody>
                                </Card>
                            </Col>
                           

                            <Col xl={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Simple Donut Chart</h4>
                                    </CardHeader>
                                    <CardBody>
                                    <SimpleDonut dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                                    </CardBody>
                                </Card>
                            </Col>
                           
                        </Row>
                       

                        <Row className="row">
                            <Col xl={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Updating Donut Chart</h4>
                                    </CardHeader>
                                    <CardBody>
                                        <div>
                                        <UpdateDonut dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                                            <div className="d-flex align-items-start flex-wrap gap-2 justify-content-center mt-4">
                                                <button id="add" className="btn btn-light btn-sm">
                                                    + ADD
                                                </button>
                                                
                                                <button id="remove" className="btn btn-light btn-sm">
                                                    - REMOVE
                                                </button>
                                                
                                                <button id="randomize" className="btn btn-light btn-sm">
                                                    RANDOMIZE
                                                </button>
                                                
                                                <button id="reset" className="btn btn-light btn-sm">
                                                    RESET
                                                </button>
                                            </div>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>                          

                            <Col xl={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Monochrome Pie Chart</h4>
                                    </CardHeader>

                                    <CardBody>
                                        <MonochromePie dataColors='["--vz-primary"]'/>
                                    </CardBody>
                                </Card>
                            </Col>                           
                        </Row>
                       

                        <Row>
                            <Col xl={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Gradient Donut Chart</h4>
                                    </CardHeader>

                                    <CardBody>
                                        <GradientDonut dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                                    </CardBody>
                                </Card>
                            </Col>                           

                            <Col xl={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Patterned Donut Chart</h4>
                                    </CardHeader>
                                    <CardBody>
                                        <PatternedDonut dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-danger", "--vz-info"]'/>
                                    </CardBody>
                                </Card>
                            </Col>                           
                        </Row>
                       

                        <Row>
                            <Col xl={6}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Pie Chart with Image Fill</h4>
                                    </CardHeader>

                                    <CardBody>
                                        <ImagePieChart dataColors='["--vz-primary", "--vz-success", "--vz-warning", "--vz-info"]'/>
                                    </CardBody>
                                </Card>
                            </Col>                           
                        </Row>                      
                        
                    </Container>
                   
                </div>
  )
}

export default PieCharts