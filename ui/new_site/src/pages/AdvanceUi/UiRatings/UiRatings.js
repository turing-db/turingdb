import React, { useState } from 'react';
import { Badge, Button, Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';

//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

// Rating
import Rating from "react-rating";

import { BasicRaterExample, RaterWithStepExample, CustomMsgExample, ReadOnlyRaterExample, OnHoverExample, ReasetRaterExample } from './UiRatingsCode';

const UiRatings = () => {
    const [customize, setcustomize] = useState("");
    const [reset, setreset] = useState("");

    document.title="Ratings | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Ratings" pageTitle="Advance UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader className="align-items-center d-flex">
                                    <h4 className="card-title mb-0 flex-grow-1">Rater Js</h4>
                                </CardHeader>
                                <CardBody>
                                    <p className="text-muted">Rater-js is a star rating widget for the browser. here are the various examples of rating shown below with variation.</p>
                                    <div className="live-preview">
                                        <div className="table-responsive">
                                            <table className="table align-middle mb-0">
                                                <thead>
                                                    <tr>
                                                        <th scope="col">Title</th>
                                                        <th scope="col">Example</th>
                                                        <th scope="col">Description</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    <tr>
                                                        <th scope="row">Basic Rater</th>
                                                        <td>
                                                            <div id="basic-rater" dir="ltr">
                                                                <Rating
                                                                    initialRating={3}
                                                                    emptySymbol="mdi mdi-star-outline text-muted "
                                                                    fullSymbol="mdi mdi-star text-warning "
                                                                />
                                                            </div>
                                                        </td>
                                                        <td>
                                                            <pre>
                                                                <code className="language-markup">
                                                                    <BasicRaterExample />
                                                                </code>
                                                            </pre>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">Rater with Step</th>
                                                        <td>
                                                            <div id="rater-step" dir="ltr" >
                                                                <Rating
                                                                    initialRating={1.5}
                                                                    fractions={2}
                                                                    emptySymbol="mdi mdi-star-outline text-muted "
                                                                    fullSymbol="mdi mdi-star text-warning "
                                                                />
                                                            </div>
                                                        </td>
                                                        <td>
                                                            <pre>
                                                                <code className="language-markup">
                                                                    <RaterWithStepExample />
                                                                </code>
                                                            </pre>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">Custom Messages</th>
                                                        <td>
                                                            <div id="rater-message" dir="ltr">
                                                                <Rating
                                                                    emptySymbol="mdi mdi-star-outline text-muted"
                                                                    fullSymbol="mdi mdi-star text-warning "
                                                                />
                                                            </div>
                                                        </td>
                                                        <td>
                                                            <pre>
                                                                <code className="language-markup">
                                                                    <CustomMsgExample />
                                                                </code>
                                                            </pre>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">Example with unlimited number of stars. readOnly option is set to true.</th>
                                                        <td>
                                                            <div id="rater-unlimitedstar" dir="ltr">
                                                                <Rating
                                                                    stop={16}
                                                                    emptySymbol="mdi mdi-star-outline text-muted fa-1x"
                                                                    fullSymbol="mdi mdi-star text-warning"
                                                                    initialRating={4.5}
                                                                    readonly
                                                                />
                                                            </div>
                                                        </td>
                                                        <td>
                                                            <pre>
                                                                <code className="language-markup">
                                                                    <ReadOnlyRaterExample />
                                                                </code>
                                                            </pre>
                                                        </td>
                                                    </tr>
                                                    <tr>
                                                        <th scope="row">On Hover Event</th>
                                                        <td>
                                                            <div dir="ltr">
                                                                <div id="rater-onhover" className="align-middle d-inline">
                                                                    <Rating
                                                                        stop={5}
                                                                        emptySymbol="mdi mdi-star-outline text-muted "
                                                                        fullSymbol="mdi mdi-star text-warning "
                                                                        onChange={(customize) => setcustomize(customize)}
                                                                    />
                                                                </div>
                                                                <Badge color="info" className="ratingnum align-middle ms-2">{customize}</Badge>
                                                            </div>
                                                        </td>
                                                        <td>
                                                            <pre>
                                                                <code className="language-markup">
                                                                    <OnHoverExample />
                                                                </code>
                                                            </pre>
                                                        </td>
                                                    </tr>

                                                    <tr>
                                                        <th scope="row">Clear/Reset Rater</th>
                                                        <td>
                                                            <div dir="ltr">
                                                                <div id="raterreset" className="align-middle d-inline">
                                                                    <Rating
                                                                        emptySymbol="mdi mdi-star-outline text-muted"
                                                                        fullSymbol={reset}
                                                                        onHover={() => setreset("mdi mdi-star text-warning")}
                                                                    />
                                                                </div>
                                                                <span className="clear-rating"></span>
                                                                <Button id="raterreset-button" color="light" size="sm" className="ms-2" onClick={() => setreset("mdi mdi-star-outline text-muted ")}>Reset</Button>
                                                            </div>
                                                        </td>
                                                        <td>
                                                            <pre>
                                                                <code className="language-markup">
                                                                    <ReasetRaterExample />
                                                                </code>
                                                            </pre>
                                                        </td>
                                                    </tr>
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre>
                                            <code className="language-markup">&lt;div data-simplebar style=&quot;max-height: 220px;&quot;&gt;
                                                ...
                                                &lt;/div&gt;</code></pre>
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

export default UiRatings;
