import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';

//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

import { HtmlHighlightExample, CssHighlightExample, JavaScriptExample } from './UiHighlightCode';

const UiHighlight = () => {
    document.title="Highlight | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Highlight" pageTitle="Advance UI" />

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader className="align-items-center d-flex">
                                    <div className="flex-grow-1">
                                        <h4 className="card-title mb-0">HTML Highlight</h4>
                                    </div>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">HTML highlight is used to mark or highlight text that is of property, relevance, or special interest to an HTML document. here is the example shown below.</p>

                                    <div className="live-preview">
                                        <pre>
                                            <code className="language-markup">
                                                <HtmlHighlightExample />
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
                                <CardHeader className="align-items-center d-flex">
                                    <div className="flex-grow-1">
                                        <h4 className="card-title mb-0">CSS Highlight</h4>
                                    </div>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">CSS highlight is used to mark or highlight text that is of property, relevance, or special interest to a CSS document. Here is the example shown below.</p>
                                    <div className="live-preview">
                                        <pre>
                                            <code className="language-css">
                                                <CssHighlightExample />
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
                                <CardHeader className="align-items-center d-flex">
                                    <div className="flex-grow-1">
                                        <h4 className="card-title mb-0">Javascript Highlight</h4>
                                    </div>
                                </CardHeader>

                                <CardBody>
                                    <p className="text-muted">Javascript highlight is used to mark or highlight text that is of property, relevance, or special interest to a Javascript document. Here is the example shown below.</p>
                                    <div className="live-preview">
                                        <pre>
                                            <code className="language-js">
                                                <JavaScriptExample />
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

export default UiHighlight;