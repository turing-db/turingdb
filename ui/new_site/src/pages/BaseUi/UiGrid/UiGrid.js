import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row, } from 'reactstrap';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

import { VerticalStartExample, VerticalCenterExample, VerticalEndExample, AlignSelfExample, HorizontalAlignExample } from "./UiGridCode";

const UiGrid = () => {
    document.title="Grid | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">               
                <Container fluid>
                    <BreadCrumb title="Grid" pageTitle="Base UI" />
                    <Row>
                        <Col className="col-12">
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Grid Options</h4>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">See how aspects of the Bootstrap grid
                                        system work across multiple devices with a handy table.</p>
                                    <div className="table-responsive">
                                        <table className="table table-bordered table-nowrap mb-0">
                                            <thead>
                                                <tr>
                                                    <th scope="col"></th>
                                                    <th scope="col" className="text-center">
                                                        xs<br />
                                                        <span className="fw-normal">&lt;576px</span>
                                                    </th>
                                                    <th scope="col" className="text-center">
                                                        sm<br />
                                                        <span className="fw-normal">≥576px</span>
                                                    </th>
                                                    <th scope="col" className="text-center">
                                                        md<br />
                                                        <span className="fw-normal">≥768px</span>
                                                    </th>
                                                    <th scope="col" className="text-center">
                                                        lg<br />
                                                        <span className="fw-normal">≥992px</span>
                                                    </th>
                                                    <th scope="col" className="text-center">
                                                        xl<br />
                                                        <span className="fw-normal">≥1200px</span>
                                                    </th>
                                                    <th scope="col" className="text-center">
                                                        xxl<br />
                                                        <span className="fw-normal">≥1400px</span>
                                                    </th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                <tr>
                                                    <th className="text-nowrap" scope="row">Grid behavior</th>
                                                    <td>Horizontal at all times</td>
                                                    <td colSpan="5">Collapsed to start, horizontal above breakpoints</td>
                                                </tr>
                                                <tr>
                                                    <th className="text-nowrap" scope="row">Max container width</th>
                                                    <td>None (auto)</td>
                                                    <td>540px</td>
                                                    <td>720px</td>
                                                    <td>960px</td>
                                                    <td>1140px</td>
                                                    <td>1320px</td>
                                                </tr>
                                                <tr>
                                                    <th className="text-nowrap" scope="row">Class prefix</th>
                                                    <td><code>col-</code></td>
                                                    <td><code>col-sm-</code></td>
                                                    <td><code>col-md-</code></td>
                                                    <td><code>col-lg-</code></td>
                                                    <td><code>col-xl-</code></td>
                                                    <td><code>col-xxl-</code></td>
                                                </tr>
                                                <tr>
                                                    <th className="text-nowrap" scope="row"># of columns</th>
                                                    <td colSpan="6">12</td>
                                                </tr>
                                                <tr>
                                                    <th className="text-nowrap" scope="row">Gutter width</th>
                                                    <td colSpan="6">24px (12px on each side of a column)</td>
                                                </tr>
                                                <tr>
                                                    <th className="text-nowrap" scope="row">Custom gutters</th>
                                                    <td colSpan="6">Yes</td>
                                                </tr>
                                                <tr>
                                                    <th className="text-nowrap" scope="row">Nestable</th>
                                                    <td colSpan="6">Yes</td>
                                                </tr>
                                                <tr>
                                                    <th className="text-nowrap" scope="row">Offsets</th>
                                                    <td colSpan="6">Yes</td>
                                                </tr>
                                                <tr>
                                                    <th className="text-nowrap" scope="row">Column ordering</th>
                                                    <td colSpan="6">Yes</td>
                                                </tr>
                                            </tbody>
                                        </table>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Vertical alignment (align-items-start)" />

                                <CardBody>
                                    <p className="text-muted">Use <code>align-items-start</code> class to vertically align-items at the start.</p>
                                    <div className="live-preview">

                                        <div className="bg-light p-3 text-center">
                                            <Row className="align-items-start" style={{ minHeight: "6rem" }}>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2 mt-2 mt-sm-0">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2 mt-2 mt-sm-0">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <VerticalStartExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Vertical alignment (align-items-center)" />

                                <CardBody>
                                    <p className="text-muted">Use <code>align-items-center</code> class to vertically align-items at the center.</p>

                                    <div className="live-preview">

                                        <div className="bg-light p-3 text-center">
                                            <Row className="align-items-center" style={{ minHeight: "6rem" }}>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2 mt-2 mt-sm-0">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2 mt-2 mt-sm-0">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <VerticalCenterExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Vertical alignment (align-items-end)" />

                                <CardBody>
                                    <p className="text-muted">Use <code>align-items-end</code> class to vertically align-items at the end.</p>
                                    <div className="live-preview">
                                        <div className="bg-light p-3 text-center">
                                            <Row className="align-items-end" style={{ minHeight: "6rem" }}>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2 mt-2 mt-sm-0">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2 mt-2 mt-sm-0">
                                                        col-sm-4
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <VerticalEndExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Align Self" />

                                <CardBody>
                                    <p className="text-muted">Use the <code>align-self-start</code>,<code>align-self-center</code>, or <code>align-self-end</code> class respectively to vertically align items with a different position.</p>

                                    <div className="live-preview">
                                        <div className="bg-light p-3 text-center">
                                            <Row className="g-2" style={{ minHeight: "9rem" }}>
                                                <div className="col-sm-4 align-self-start">
                                                    <div className="bg-soft-primary text-primary rounded p-2">
                                                        align-self-start
                                                    </div>
                                                </div>
                                                <div className="col-sm-4 align-self-center">
                                                    <div className="bg-soft-primary text-primary rounded p-2">
                                                        align-self-center
                                                    </div>
                                                </div>
                                                <div className="col-sm-4 align-self-end">
                                                    <div className="bg-soft-primary text-primary rounded p-2">
                                                        align-self-end
                                                    </div>
                                                </div>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <AlignSelfExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Horizontal Alignment" />

                                <CardBody>
                                    <p className="text-muted">Use the <code>justify-content-start</code>,<code>justify-content-center</code>, or <code>justify-content-end</code> class respectively to horizontally align items with a different position.</p>
                                    <div className="live-preview">
                                        <div className="bg-light p-3 text-center">
                                            <Row className="justify-content-start">
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2">
                                                        justify-content-start
                                                    </div>
                                                </Col>
                                            </Row>
                                            <Row className="justify-content-center">
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2 mt-2 mt-sm-0">
                                                        justify-content-center
                                                    </div>
                                                </Col>
                                            </Row>
                                            <Row className="justify-content-end">
                                                <Col sm={4}>
                                                    <div className="bg-soft-primary text-primary rounded p-2 mt-2 mt-sm-0">
                                                        justify-content-end
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <HorizontalAlignExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div >
        </React.Fragment >
    );
};

export default UiGrid;
