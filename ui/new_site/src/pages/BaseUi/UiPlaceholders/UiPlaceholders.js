import React from 'react';
import { Card, CardBody, Col, Container, Row, } from 'reactstrap';
import { Link } from 'react-router-dom';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Images
import img1 from "../../../assets/images/small/img-1.jpg";
import img2 from "../../../assets/images/small/img-2.jpg";

import { DefaultPlaceholderExample, WidthExample, SizingExample, ColorExample } from './UiPlaceholdersCode';

const UiPlaceholders = () => {
    document.title="Placeholders | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">               
                <Container fluid>
                    <BreadCrumb title="Placeholders" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Default Placeholder" />
                                <CardBody>

                                    <p className="text-muted">In the example below, we take a typical card component and recreate it with placeholders applied to create a “loading card”.</p>

                                    <div className="live-preview">
                                        <Row className="justify-content-center">
                                            <Col xl={7}>
                                                <Row className="justify-content-between">
                                                    <Col lg={5} sm={6}>
                                                        <Card>
                                                            <img src={img1} className="card-img-top" alt="card img" />

                                                            <CardBody>
                                                                <h5 className="card-title">Card title</h5>
                                                                <p className="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
                                                                <Link to="#" className="btn btn-primary">Go somewhere</Link>
                                                            </CardBody>
                                                        </Card>
                                                    </Col>
                                                    <Col lg={5} sm={6}>
                                                        <Card>
                                                            <img src={img2} className="card-img-top" alt="card dummy img" />
                                                            <CardBody>
                                                                <h5 className="card-title placeholder-glow">
                                                                    <span className="placeholder col-6"></span>
                                                                </h5>
                                                                <p className="card-text placeholder-glow">
                                                                    <span className="placeholder col-7"></span>
                                                                    <span className="placeholder col-4"></span>
                                                                    <span className="placeholder col-4"></span>
                                                                    <span className="placeholder col-6"></span>
                                                                </p>
                                                                <Link to="#" tabIndex="-1" className="btn btn-primary disabled placeholder col-6"></Link>
                                                            </CardBody>
                                                        </Card>
                                                    </Col>
                                                </Row>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <DefaultPlaceholderExample />
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
                                <PreviewCardHeader title="Width" />
                                <CardBody>
                                    <p className="text-muted">Use  <code>w-25,w-50,w-75</code> or <code>w-100</code> class to placeholder class to set different widths to the placeholder.</p>
                                    <div className="live-preview">
                                        <span className="placeholder w-50"></span>
                                        <span className="placeholder w-75"></span>
                                        <span className="placeholder w-25"></span>
                                        <span className="placeholder w-100"></span>
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
                                <PreviewCardHeader title="Sizing" />
                                <CardBody>
                                    <p className="text-muted">Use <code>placeholder-lg</code>, <code>placeholder-sm</code>, or <code>placeholder-xs</code> class to placeholder class to set different size placeholder.</p>
                                    <div className="live-preview">
                                        <Row className="gap-0">
                                            <Col className="col-12">
                                                <span className="placeholder placeholder-lg w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder placeholder-sm w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder placeholder-xs w-100"></span>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <SizingExample />
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
                                <PreviewCardHeader title="Color" />
                                <CardBody>
                                    <p className="text-muted">Use <code>bg-</code> class with the below-mentioned color variation to set a custom color.</p>
                                    <div className="live-preview">
                                        <Row className="g-2">
                                            <Col className="col-12">
                                                <span className="placeholder w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder bg-primary w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder bg-secondary w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder bg-success w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder bg-danger w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder bg-warning w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder bg-info w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder bg-light w-100"></span>
                                            </Col>
                                            <Col className="col-12">
                                                <span className="placeholder bg-dark w-100"></span>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ColorExample />
                                            </code>
                                        </pre>
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

export default UiPlaceholders;
