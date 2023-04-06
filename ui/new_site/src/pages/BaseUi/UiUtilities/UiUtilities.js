import React from 'react';
import { Button, Card, CardBody, CardHeader, Col, Container, Input, Row, } from 'reactstrap';
import { Link } from 'react-router-dom';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Images
import avatar1 from "../../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../../assets/images/users/avatar-2.jpg";

import img2 from "../../../assets/images/small/img-2.jpg";
import { StacksVerticalExample, StacksHorizontalExample, TextSelectionExample, PointerEventsExample, OverflowExample, PositionExample, ShadowsExample, WidthExample, HeightExample } from './UiUtilitiesCode';

const UiUtilities = () => {
    document.title = "Utilities | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Utilities" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Borders</h4>
                                </CardHeader>

                                <CardBody>

                                    <p className="text-muted">Use border utilities to add or remove an element’s borders.
                                        Choose from all borders or one at a time.</p>

                                    <div className="live-preview">
                                        <div>
                                            <div>
                                                <h5 className="fs-13 mb-3">Border width</h5>
                                                <div className="d-flex align-items-start flex-wrap gap-4">
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-1 border-light avatar-md bg-transparent mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border.border-1</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-2 border-light avatar-md bg-transparent mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border.border-2</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-3 border-light avatar-md bg-transparent mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border.border-3</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-4 border-light avatar-md bg-transparent mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border.border-4</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-5 border-light avatar-md bg-transparent mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border.border-5</code></p>
                                                    </div>
                                                </div>
                                            </div>
                                            <Row className="mt-4">
                                                <Col lg={6}>
                                                    <div>
                                                        <h5 className="fs-13 mb-3">Additive</h5>
                                                        <Row>
                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border avatar-md border-success bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-top avatar-md border-success bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-top</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-end avatar-md border-success bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-end</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-bottom avatar-md border-success bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-bottom</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-start avatar-md border-success bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-start</code></p>
                                                                </div>
                                                            </Col>

                                                        </Row>

                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div>
                                                        <h5 className="fs-13 mb-3">Subtractive</h5>
                                                        <Row>
                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-0 border-success avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-0</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border border-success border-top-0 avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-top-0</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border border-success border-end-0 avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-end-0</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border border-success border-bottom-0 avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-bottom-0</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col xl={2} sm={4} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border border-start-0 border-success avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-start-0</code></p>
                                                                </div>
                                                            </Col>

                                                        </Row>

                                                    </div>
                                                </Col>
                                            </Row>
                                            <div className="mt-4">
                                                <h5 className="fs-13 mb-3">Border Color</h5>
                                                <div className="d-flex align-items-start flex-wrap gap-4">
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-primary avatar-md bg-light mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border-primary</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-secondary avatar-md bg-light mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border-secondary</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-success avatar-md bg-light mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border-success</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div className="border border-info avatar-md bg-light mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border-info</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-warning avatar-md bg-light mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border-warning</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div
                                                            className="border border-danger avatar-md bg-light mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border-danger</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div className="border border-dark avatar-md bg-light mx-auto mb-1">
                                                        </div>
                                                        <p><code>.border-dark</code></p>
                                                    </div>
                                                </div>
                                            </div>
                                            <Row className="mt-4">
                                                <Col xxl={6}>
                                                    <div>
                                                        <h5 className="fs-13 mb-3">Border Style</h5>
                                                        <div className="d-flex align-items-start flex-wrap gap-4">
                                                            <div className="text-center">
                                                                <div
                                                                    className="border border-primary border-dotted avatar-md bg-light mx-auto mb-1">
                                                                </div>
                                                                <p><code>.border-dotted</code></p>
                                                            </div>
                                                            <div className="text-center">
                                                                <div
                                                                    className="border border-secondary border-dashed avatar-md bg-light mx-auto mb-1">
                                                                </div>
                                                                <p><code>.border-dashed</code></p>
                                                            </div>
                                                            <div className="text-center">
                                                                <div
                                                                    className="border border-success border-groove avatar-md bg-light mx-auto mb-1">
                                                                </div>
                                                                <p><code>.border-groove</code></p>
                                                            </div>
                                                            <div className="text-center">
                                                                <div
                                                                    className="border border-info border-outset avatar-md bg-light mx-auto mb-1">
                                                                </div>
                                                                <p><code>.border-outset</code></p>
                                                            </div>
                                                            <div className="text-center">
                                                                <div
                                                                    className="border border-warning border-ridge avatar-md bg-light mx-auto mb-1">
                                                                </div>
                                                                <p><code>.border-ridge</code></p>
                                                            </div>
                                                            <div className="text-center">
                                                                <div
                                                                    className="border border-danger border-inset avatar-md bg-light mx-auto mb-1">
                                                                </div>
                                                                <p><code>.border-inset</code></p>
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Col>


                                                <Col xxl={6}>
                                                    <div>
                                                        <h5 className="fs-13 mb-3">Additive</h5>
                                                        <Row>
                                                            <Col sm={3} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-start border-start-dashed border-success avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-start-dashed</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col sm={3} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-top border-top-dashed border-success avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-top-dashed</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col sm={3} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-end border-end-dashed border-success avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-end-dashed</code></p>
                                                                </div>
                                                            </Col>

                                                            <Col sm={3} className="col">
                                                                <div className="text-center">
                                                                    <div
                                                                        className="border-bottom border-bottom-dashed border-success avatar-md bg-light mx-auto mb-1">
                                                                    </div>
                                                                    <p><code>.border-bottom-dashed</code></p>
                                                                </div>
                                                            </Col>

                                                        </Row>
                                                    </div>
                                                </Col>
                                            </Row>
                                            <div className="mt-4">
                                                <h5 className="fs-13 mb-3">Border Opacity</h5>
                                                <div className="d-flex align-items-start flex-wrap gap-4">
                                                    <div className="text-center">
                                                        <div className="border border-success border-opacity-10 avatar-md bg-light mx-auto mb-1"></div>
                                                        <p><code>.border-opacity-10</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div className="border border-success border-opacity-25 avatar-md bg-light mx-auto mb-1"></div>
                                                        <p><code>.border-opacity-25</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div className="border border-success border-opacity-50 avatar-md bg-light mx-auto mb-1"></div>
                                                        <p><code>.border-opacity-50</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div className="border border-success border-opacity-75 avatar-md bg-light mx-auto mb-1"></div>
                                                        <p><code>.border-opacity-75</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <div className="border border-success border-opacity-100 avatar-md bg-light mx-auto mb-1"></div>
                                                        <p><code>.border-opacity-100</code></p>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Border Radius</h4>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">Use below-mentioned class to an element to easily round its
                                        corners.</p>
                                    <div className="live-preview">
                                        <div>
                                            <div>
                                                <div className="d-flex align-items-start flex-wrap gap-4">
                                                    <div className="text-center">
                                                        <img src={avatar2}
                                                            className="avatar-md rounded" alt="rounded" />
                                                        <p><code>.rounded</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar2}
                                                            className="avatar-md rounded-top" alt="rounded-top" />
                                                        <p><code>.rounded-top</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar2}
                                                            className="avatar-md rounded-end" alt="rounded-end" />
                                                        <p><code>.rounded-end</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar2}
                                                            className="avatar-md rounded-bottom" alt="rounded-bottom" />
                                                        <p><code>.rounded-bottom</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar2}
                                                            className="avatar-md rounded-start" alt="rounded-start" />
                                                        <p><code>.rounded-start</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar2}
                                                            className="avatar-md rounded-circle" alt="rounded-circle" />
                                                        <p><code>.rounded-circle</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar2}
                                                            className="avatar-md rounded-top" alt="rounded-top" />
                                                        <p><code>.rounded-top</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={img2}
                                                            className="avatar-md w-auto rounded-pill" alt="rounded-pill" />
                                                        <p><code>.rounded-pill</code></p>
                                                    </div>
                                                </div>
                                            </div>
                                            <div>
                                                <h5 className="fs-13 mb-3">Sizes</h5>
                                                <div className="d-flex align-items-start flex-wrap gap-4">
                                                    <div className="text-center">
                                                        <img src={avatar1}
                                                            className="avatar-md rounded-0" alt="rounded-0" />
                                                        <p><code>.rounded-0</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar1}
                                                            className="avatar-md rounded-1" alt="rounded-1" />
                                                        <p><code>.rounded-1</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar1}
                                                            className="avatar-md rounded-2" alt="rounded-2" />
                                                        <p><code>.rounded-2</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar1}
                                                            className="avatar-md rounded-3" alt="rounded-3" />
                                                        <p><code>.rounded-3</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar1} className="avatar-md rounded-4" alt="rounded-4" />
                                                        <p><code>.rounded-4</code></p>
                                                    </div>
                                                    <div className="text-center">
                                                        <img src={avatar1} className="avatar-md rounded-5" alt="rounded-5" />
                                                        <p><code>.rounded-5</code></p>
                                                    </div>
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Display Property</h4>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">Use below-mentioned class to an element to display the value
                                        of components and more with our display utilities.</p>
                                    <div className="live-preview">
                                        <div>
                                            <Row>
                                                <Col lg={6}>
                                                    <div>
                                                        <h5 className="fs-13 mb-3">Display classes</h5>
                                                        <div className="table-responsive">
                                                            <table className="table table-bordered">
                                                                <thead className="table-light">
                                                                    <tr>
                                                                        <th scope="col" style={{ width: "40%" }}>Display
                                                                            Property</th>
                                                                        <th scope="col">Class</th>
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    <tr>
                                                                        <td>Display : none</td>
                                                                        <td><code>.d-none</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Display : inline</td>
                                                                        <td><code>.d-inline</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Display : inline-block</td>
                                                                        <td><code>.d-inline-block</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Display : block</td>
                                                                        <td><code>.d-block</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Display : table</td>
                                                                        <td><code>.d-table</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Display : table-cell</td>
                                                                        <td><code>.d-table-cell</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Display : table-row</td>
                                                                        <td><code>.d-table-row</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Display : flex</td>
                                                                        <td><code>.d-flex</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Display : inline-flex</td>
                                                                        <td><code>.d-inline-flex</code></td>
                                                                    </tr>
                                                                </tbody>
                                                            </table>
                                                        </div>
                                                    </div>
                                                </Col>

                                                <Col lg={6}>
                                                    <div>
                                                        <h5 className="fs-13 mb-3">Hiding elements</h5>
                                                        <div className="table-responsive">
                                                            <table className="table table-bordered">
                                                                <thead className="table-light">
                                                                    <tr>
                                                                        <th scope="col">Screen Size</th>
                                                                        <th scope="col">Class</th>
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    <tr>
                                                                        <td>Hidden on all</td>
                                                                        <td><code>.d-none</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Hidden only on xs</td>
                                                                        <td><code>.d-none .d-sm-block</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Hidden only on sm</td>
                                                                        <td><code>.d-sm-none .d-md-block</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Hidden only on md</td>
                                                                        <td><code>.d-md-none .d-lg-block</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Hidden only on lg</td>
                                                                        <td><code>.d-lg-none .d-xl-block</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Hidden only on xl</td>
                                                                        <td><code>.d-xl-none</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Visible on all</td>
                                                                        <td><code>.d-block</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Visible only on xs</td>
                                                                        <td><code>.d-block .d-sm-none</code></td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Visible only on sm</td>
                                                                        <td><code>.d-none .d-sm-block .d-md-none</code>
                                                                        </td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Visible only on md</td>
                                                                        <td><code>.d-none .d-md-block .d-lg-none</code>
                                                                        </td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Visible only on lg</td>
                                                                        <td><code>.d-none .d-lg-block .d-xl-none</code>
                                                                        </td>
                                                                    </tr>
                                                                    <tr>
                                                                        <td>Visible only on xl</td>
                                                                        <td><code>.d-none .d-xl-block</code></td>
                                                                    </tr>
                                                                </tbody>
                                                            </table>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                        <div className="mt-4">
                                            <h5 className="fs-13">Display in print</h5>
                                            <p className="text-muted">Change the display value of elements when printing
                                                with our print display utility classes.</p>

                                            <ul className="ps-4 mb-0">
                                                <li className="py-1"><code>.d-print-none</code></li>
                                                <li className="py-1"><code>.d-print-inline</code></li>
                                                <li className="py-1"><code>.d-print-inline-block</code></li>
                                                <li className="py-1"><code>.d-print-block</code></li>
                                                <li className="py-1"><code>.d-print-table</code></li>
                                                <li className="py-1"><code>.d-print-table-row</code></li>
                                                <li className="py-1"><code>.d-print-table-cell</code></li>
                                                <li className="py-1"><code>.d-print-flex</code></li>
                                                <li className="py-1"><code>.d-print-inline-flex</code></li>
                                            </ul>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Custom Class</h4>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">Use the below-mentioned class to an element to custom display
                                        the value of components and more with our display utilities.</p>
                                    <div className="live-preview">
                                        <div>
                                            <div className="table-responsive">
                                                <table className="table table-bordered mb-0">
                                                    <thead className="table-light">
                                                        <tr>
                                                            <th scope="col" style={{ width: "40%" }}>Property</th>
                                                            <th scope="col">Class</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        <tr>
                                                            <td>font-family: 'IBM Plex Sans', sans-serif;</td>
                                                            <td><code>.ff-base</code></td>
                                                        </tr>
                                                        <tr>
                                                            <td>font-family: 'IBM Plex Sans', sans-serif;</td>
                                                            <td><code>.ff-secondary</code></td>
                                                        </tr>
                                                        <tr>
                                                            <td>font-weight: 500;</td>
                                                            <td><code>.fw-medium</code></td>
                                                        </tr>
                                                        <tr>
                                                            <td>min-width: 80px;</td>
                                                            <td><code>.w-xs</code></td>
                                                        </tr>
                                                        <tr>
                                                            <td>min-width: 95px;</td>
                                                            <td><code>.w-sm</code></td>
                                                        </tr>
                                                        <tr>
                                                            <td>min-width: 110px;</td>
                                                            <td><code>.w-md</code></td>
                                                        </tr>
                                                        <tr>
                                                            <td>min-width: 140px;</td>
                                                            <td><code>.w-lg</code></td>
                                                        </tr>
                                                        <tr>
                                                            <td>min-width: 160px;</td>
                                                            <td><code>.w-xl</code></td>
                                                        </tr>
                                                    </tbody>
                                                </table>
                                            </div>
                                        </div>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Stacks - Vertical" />
                                <CardBody>
                                    <p className="text-muted mb-3">Use <code>vstack</code> class to create vertical layouts.
                                        Stacked items are full-width by default. Use <code>gap-</code> class with
                                        variation to add space between items.</p>
                                    <div className="live-preview">
                                        <div className="vstack gap-3">
                                            <div className="bg-light border p-1 px-2">First item</div>
                                            <div className="bg-light border p-1 px-2">Second item</div>
                                            <div className="bg-light border p-1 px-2">Third item</div>
                                        </div>

                                        <div className="mt-5">
                                            <h5 className="fs-14 mb-3">Vertical Stacks Examples</h5>
                                            <div className="vstack gap-2 col-md-5 mx-auto">
                                                <Button color="primary">Save changes</Button>
                                                <Button color="secondary" outline>Cancel</Button>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <StacksVerticalExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Stacks - Horizontal" />

                                <CardBody>

                                    <p className="text-muted mb-3">Use <code>hstack</code> class to create horizontal
                                        layouts. Stacked items are full-width by default. Use <code>gap-</code> class
                                        with variation to add space between items.</p>

                                    <div className="live-preview">
                                        <div className="hstack gap-3">
                                            <div className="bg-light border p-1 px-2">First item</div>
                                            <div className="bg-light border p-1 px-2">Second item</div>
                                            <div className="bg-light border p-1 px-2">Third item</div>
                                        </div>
                                        <div className="mt-4">
                                            <p className="text-muted mb-3">Using horizontal margin utilities like
                                                <code>ms-auto</code> as spacers:</p>

                                            <div className="hstack gap-3">
                                                <div className="bg-light border p-1 px-2">First item</div>
                                                <div className="bg-light border p-1 px-2 ms-auto">Second item</div>
                                                <div className="bg-light border p-1 px-2">Third item</div>
                                            </div>
                                            <div className="hstack gap-3 mt-3">
                                                <div className="bg-light border p-1 px-2">First item</div>
                                                <div className="vr"></div>
                                                <div className="bg-light border p-1 px-2">Second item</div>

                                                <div className="bg-light border p-1 px-2 ms-auto">Third item</div>
                                            </div>
                                        </div>

                                        <div className="mt-5">
                                            <h5 className="fs-13 mb-3">Horizontal Stacks Examples</h5>
                                            <div className="hstack gap-3">
                                                <Input className="form-control me-auto" type="text"
                                                    placeholder="Add your item here..." />
                                                <Button color="primary">Submit</Button>
                                                <div className="vr"></div>
                                                <Button color="secondary" outline>Reset</Button>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <StacksHorizontalExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Text Selection" />


                                <CardBody>
                                    <p className="text-muted mb-3">Use <code>user-select-all</code>,
                                        <code>user-select-auto</code>, or <code>user-select-none</code> class to the
                                        content which is selected when the user interacts with it.</p>

                                    <div className="live-preview">
                                        <div>
                                            <p className="user-select-all">This paragraph will be entirely selected when
                                                clicked by the user.</p>
                                            <p className="user-select-auto">This paragraph has default select behavior.</p>
                                            <p className="user-select-none mb-0">This paragraph will not be selectable when
                                                clicked by the user.</p>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <TextSelectionExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Pointer Events" />

                                <CardBody>

                                    <p className="text-muted mb-3">Use <code>pe-none</code> class to prevent and
                                        <code>pe-auto</code> class to add element interactions respectively.</p>

                                    <div className="live-preview">
                                        <div>
                                            <p><Link to="#" className="text-success pe-none" tabIndex="-1">This link</Link> can
                                                not be clicked.</p>
                                            <p><Link to="#" className="text-success pe-auto">This link</Link> can be clicked
                                                (this is default behavior).</p>
                                            <p className="pe-none mb-0"><Link to="#" className="text-success" tabIndex="-1">This
                                                link</Link> can not be clicked because the <code>pointer-events</code>
                                                property is inherited from its parent. However, <Link to="#"
                                                    className="pe-auto">this link</Link> has a <code>pe-auto</code> class and
                                                can be clicked.</p>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <PointerEventsExample />
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

                                <PreviewCardHeader title="Overflow" />

                                <CardBody>

                                    <p className="text-muted">Use <code>overflow</code> property on the fly with four
                                        default values and classes.</p>
                                    <div className="live-preview">
                                        <div>
                                            <Row>
                                                <Col xl={3} sm={6}>
                                                    <div className="mt-4 mt-xl-0">
                                                        <h5 className="fs-13">Overflow Auto</h5>

                                                        <div className="d-md-flex bg-light">
                                                            <div className="overflow-auto p-3 mb-0 bg-light"
                                                                style={{ height: "100px" }}>
                                                                This is an example of using <code>.overflow-auto</code>
                                                                on an element with set width and height dimensions. By
                                                                design, this content will vertically scroll. Lorem ipsum
                                                                dolor sit amet, consectetuer adipiscing elit. Aenean
                                                                commodo ligula eget dolor. Aenean massa. Cum sociis
                                                                natoque penatibus et
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Col>

                                                <Col xl={3} sm={6}>
                                                    <div className="mt-4 mt-xl-0">
                                                        <h5 className="fs-13">Overflow Hidden</h5>

                                                        <div className="d-md-flex bg-light">
                                                            <div className="overflow-hidden p-3 mb-0 bg-light"
                                                                style={{ height: "100px" }}>
                                                                This is an example of using
                                                                <code>.overflow-hidden</code> on an element with set
                                                                width and height dimensions. Lorem ipsum dolor sit amet,
                                                                consectetur adipiscing elit, sed do eiusmod tempor
                                                                incididunt ut labore et dolore magna aliqua.
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Col>
                                                <Col xl={3} sm={6}>
                                                    <div className="mt-4 mt-xl-0">
                                                        <h5 className="fs-13">Overflow Visible</h5>

                                                        <div className="d-md-flex bg-light">
                                                            <div className="overflow-visible p-3 mb-0 bg-light"
                                                                style={{ height: "100px" }}>
                                                                This is an example of using
                                                                <code>.overflow-visible</code> on an element with set
                                                                width and height dimensions.Lorem ipsum dolor sit amet,
                                                                consectetur adipiscing elit, sed do eiusmod tempor
                                                                incididunt ut labore et dolore magna aliqua.
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Col>
                                                <Col xl={3} sm={6}>
                                                    <div className="mt-4 mt-xl-0">
                                                        <h5 className="fs-13">Overflow Scroll</h5>

                                                        <div className="d-md-flex bg-light">
                                                            <div className="overflow-scroll p-3 mb-0 bg-light"
                                                                style={{ height: "100px" }}>
                                                                This is an example of using
                                                                <code>.overflow-scroll</code> on an element with set
                                                                width and height dimensions. Lorem ipsum dolor sit amet,
                                                                consectetur adipiscing elit, sed do eiusmod tempor
                                                                incididunt ut labore et dolore magna aliqua.
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>

                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <OverflowExample />
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

                                <PreviewCardHeader title="Position" />

                                <CardBody>
                                    {/* <p className="text-muted">Use below mentioned classes to arrange elements easily with
                                            the edge positioning utilities.The format is {property}-{position}.</p> */}
                                    <div className="live-preview">
                                        <div>
                                            <Row>
                                                <Col xl={4} sm={6}>
                                                    <div>
                                                        <h5 className="fs-13">Arrange elements</h5>

                                                        <div className="position-relative p-5 bg-light m-3 border rounded"
                                                            style={{ height: "180px" }}>
                                                            <div
                                                                className="position-absolute top-0 start-0 avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-0 end-0 avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-50 start-50 avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute bottom-50 end-50 avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute bottom-0 start-0 avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute bottom-0 end-0 avatar-xs bg-dark rounded">
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Col>
                                                <Col xl={4} sm={6}>
                                                    <div className="mt-4 mt-sm-0">
                                                        <h5 className="fs-13 mb-4">Center elements</h5>

                                                        <div className="position-relative m-3 bg-light border rounded"
                                                            style={{ height: "180px" }}>
                                                            <div
                                                                className="position-absolute top-0 start-0 translate-middle avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-0 start-50 translate-middle avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-0 start-100 translate-middle avatar-xs bg-dark rounded">
                                                            </div>

                                                            <div
                                                                className="position-absolute top-50 start-0 translate-middle avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-50 start-50 translate-middle avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-50 start-100 translate-middle avatar-xs bg-dark rounded">
                                                            </div>

                                                            <div
                                                                className="position-absolute top-100 start-0 translate-middle avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-100 start-50 translate-middle avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-100 start-100 translate-middle avatar-xs bg-dark rounded">
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Col>
                                                <Col xl={4} sm={6}>
                                                    <div className="mt-4 mt-sm-0">
                                                        <h5 className="fs-13">Center elements</h5>

                                                        <div className="position-relative m-3 bg-light border rounded"
                                                            style={{ height: "180px" }}>
                                                            <div
                                                                className="position-absolute top-0 start-0 avatar-xs bg-dark rounded ">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-0 start-50 translate-middle-x avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-0 end-0 avatar-xs bg-dark rounded">
                                                            </div>

                                                            <div
                                                                className="position-absolute top-50 start-0 translate-middle-y avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-50 start-50 translate-middle avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute top-50 end-0 translate-middle-y avatar-xs bg-dark rounded">
                                                            </div>

                                                            <div
                                                                className="position-absolute bottom-0 start-0 avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute bottom-0 start-50 translate-middle-x avatar-xs bg-dark rounded">
                                                            </div>
                                                            <div
                                                                className="position-absolute bottom-0 end-0 avatar-xs bg-dark rounded">
                                                            </div>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>

                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <PositionExample />
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

                                <PreviewCardHeader title="Shadows" />


                                <CardBody>
                                    <p className="text-muted">While shadows on components are disabled by default in
                                        Bootstrap and can be enabled via <code>$enable-shadows</code>, you can also
                                        quickly add or remove a shadow with our <code>box-shadow</code> utility classes.
                                        Includes support for <code>.shadow-none</code> and three default sizes (which
                                        have associated variables to match).</p>
                                    <div className="live-preview">
                                        <Row className="gy-3">
                                            <Col md={6}>
                                                <div className="shadow-none p-3 bg-light rounded">No shadow</div>
                                            </Col>
                                            <Col md={6}>
                                                <div className="shadow-sm p-3 bg-light rounded">Small shadow</div>
                                            </Col>
                                            <Col md={6}>
                                                <div className="shadow p-3 bg-light rounded">Regular shadow</div>
                                            </Col>
                                            <Col md={6}>
                                                <div className="shadow-lg p-3 bg-light rounded">Larger shadow</div>
                                            </Col>
                                        </Row>

                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ShadowsExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                                {/* <!-- end card-body --> */}
                            </Card>
                            {/* <!-- end card --> */}
                        </Col>

                        {/* <!-- end col --> */}
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Width" />

                                <CardBody>
                                    <p className="text-muted">Use
                                        <code>w-25</code>,<code>w-50</code>,<code>w-75</code>,<code>w-100</code> or
                                        <code>w-auto</code> class to set width <code>25%</code>, <code>50%</code>,
                                        <code>75%</code>, <code>100%</code>, or <code>auto</code> by default
                                        respectively.</p>
                                    <div className="live-preview">
                                        <div>
                                            <div className="w-25 p-3 bg-light">Width 25%</div>
                                            <div className="w-50 p-3 bg-light">Width 50%</div>
                                            <div className="w-75 p-3 bg-light">Width 75%</div>
                                            <div className="w-100 p-3 bg-light">Width 100%</div>
                                            <div className="w-auto p-3 bg-light">Width auto</div>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <WidthExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>

                                <PreviewCardHeader title="Height" />

                                <CardBody>
                                    <p className="text-muted">Use
                                        <code>h-25</code>,<code>h-50</code>,<code>h-75</code>,<code>h-100</code> or
                                        <code>h-auto</code> class to set height <code>25%</code>, <code>50%</code>,
                                        <code>75%</code>, <code>100%</code>, or <code>auto</code> by default
                                        respectively.</p>
                                    <div className="live-preview">
                                        <div className="row text-center" style={{ height: "255px" }}>
                                            <Col className="col">
                                                <div className="h-25 p-3 bg-light">Height25%</div>
                                            </Col>
                                            <Col className="col">
                                                <div className="h-50 p-3 bg-light">Height 50%</div>
                                            </Col>
                                            <Col className="col">
                                                <div className="h-75 p-3 bg-light">Height 75%</div>
                                            </Col>
                                            <Col className="col">
                                                <div className="h-100 p-3 bg-light">Height 100%</div>
                                            </Col>
                                            <Col className="col">
                                                <div className="h-auto p-3 bg-light">Height auto</div>
                                            </Col>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <HeightExample />
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

export default UiUtilities;
