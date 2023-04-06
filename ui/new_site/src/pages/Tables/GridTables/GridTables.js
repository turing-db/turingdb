import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';

// Import Table Data
import { BaseExample, CardTableExample, PaginationExample, SearchExample, SortingExample, LoadingStateExample, FixedHeaderExample, HiddenColumnsExample } from './GridTablesData';

const GridTables = () => {
  document.title="Grid Js | Velzon - React Admin & Dashboard Template";

    return (
        <React.Fragment>
            <div className="page-content">
                
                <Container fluid>
                    <BreadCrumb title="Grid Js" pageTitle="Tables" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0 flex-grow-1">Base Example</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="table-gridjs">
                                        <BaseExample />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Card Table</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="table-card" className="table-card">
                                        <CardTableExample />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Pagination</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="table-pagination">
                                        <PaginationExample />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Search</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="table-search">
                                        <SearchExample />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Sorting</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="table-sorting">
                                        <SortingExample />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Loading State</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="table-loading-state">
                                        <LoadingStateExample />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Fixed Header</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="table-fixed-header">
                                        <FixedHeaderExample />
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Hidden Columns</h4>
                                </CardHeader>

                                <CardBody>
                                    <div id="table-hidden-column">
                                        <HiddenColumnsExample />
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

export default GridTables;