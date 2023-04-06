import React from 'react';
import { Col, Container, Form, Row } from 'reactstrap';

const Contact = () => {
    return (
        <React.Fragment>
            <section className="section" id="contact">
                <Container>
                    <Row className="justify-content-center">
                        <Col lg={8}>
                            <div className="text-center mb-5">
                                <h3 className="mb-3 fw-semibold">Get In Touch</h3>
                                <p className="text-muted mb-4 ff-secondary">We thrive when coming up with innovative ideas but also
                                    understand that a smart concept should be supported with faucibus sapien odio measurable
                                    results.</p>
                            </div>
                        </Col>
                    </Row>

                    <Row className="gy-4">
                        <Col lg={4}>
                            <div>
                                <div className="mt-4">
                                    <h5 className="fs-13 text-muted text-uppercase">Office Address 1:</h5>
                                    <div className="ff-secondary fw-semibold">4461 Cedar Street Moro, <br />AR 72368</div>
                                </div>
                                <div className="mt-4">
                                    <h5 className="fs-13 text-muted text-uppercase">Office Address 2:</h5>
                                    <div className="ff-secondary fw-semibold">2467 Swick Hill Street <br />New Orleans, LA</div>
                                </div>
                                <div className="mt-4">
                                    <h5 className="fs-13 text-muted text-uppercase">Working Hours:</h5>
                                    <div className="ff-secondary fw-semibold">9:00am to 6:00pm</div>
                                </div>
                            </div>
                        </Col>

                        <Col lg={8}>
                            <div>
                                <Form>
                                    <Row>
                                        <Col lg={6}>
                                            <div className="mb-4">
                                                <label htmlFor="name" className="form-label fs-13">Name</label>
                                                <input name="name" id="name" type="text"
                                                    className="form-control bg-light border-light" placeholder="Your name*" />
                                            </div>
                                        </Col>
                                        <Col lg={6}>
                                            <div className="mb-4">
                                                <label htmlFor="email" className="form-label fs-13">Email</label>
                                                <input name="email" id="email" type="email"
                                                    className="form-control bg-light border-light" placeholder="Your email*" />
                                            </div>
                                        </Col>
                                    </Row>
                                    <Row>
                                        <Col lg={12}>
                                            <div className="mb-4">
                                                <label htmlFor="subject" className="form-label fs-13">Subject</label>
                                                <input type="text" className="form-control bg-light border-light" id="subject"
                                                    name="subject" placeholder="Your Subject.." />
                                            </div>
                                        </Col>
                                    </Row>
                                    <Row>
                                        <Col lg={12}>
                                            <div className="mb-3">
                                                <label htmlFor="comments" className="form-label fs-13">Message</label>
                                                <textarea name="comments" id="comments" rows="3"
                                                    className="form-control bg-light border-light"
                                                    placeholder="Your message..."></textarea>
                                            </div>
                                        </Col>
                                    </Row>
                                    <Row>
                                        <Col lg={12} className="text-end">
                                            <input type="submit" id="submit" name="send" className="submitBnt btn btn-primary"
                                                value="Send Message" />
                                        </Col>
                                    </Row>
                                </Form>
                            </div>
                        </Col>
                    </Row>
                </Container>
            </section>
        </React.Fragment>
    );
};

export default Contact;