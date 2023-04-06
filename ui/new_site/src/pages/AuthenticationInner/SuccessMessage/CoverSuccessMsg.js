import React from 'react';
import { Link } from 'react-router-dom';
import { Card, Col, Container, Row } from 'reactstrap';

import AuthSlider from '../authCarousel';

const CoverSuccessMsg = () => {
    document.title="Success Message | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>            
            <div className="auth-page-wrapper auth-bg-cover py-5 d-flex justify-content-center align-items-center min-vh-100">
                <div className="bg-overlay"></div>
                <div className="auth-page-content overflow-hidden pt-lg-5">
                    <Container>
                        <Row>
                            <Col lg={12}>
                                <Card className="overflow-hidden">
                                    <Row className="justify-content-center g-0">
                                        <AuthSlider />

                                        <Col lg={6}>
                                            <div className="p-lg-5 p-4 text-center">
                                                <div className="avatar-lg mx-auto mt-2">
                                                    <div className="avatar-title bg-light text-success display-3 rounded-circle">
                                                        <i className="ri-checkbox-circle-fill"></i>
                                                    </div>
                                                </div>
                                                <div className="mt-4 pt-2">
                                                    <h4>Well done !</h4>
                                                    <p className="text-muted mx-4">Aww yeah, you successfully read this important message.</p>
                                                    <div className="mt-4">
                                                        <Link to="/auth-signin-basic" className="btn btn-success w-100">Back to Dashboard</Link>
                                                    </div>
                                                </div>
                                            </div>
                                        </Col>
                                    </Row>
                                </Card>
                            </Col>
                        </Row>
                    </Container>
                </div>

                <footer className="footer start-0">
                    <Container>
                        <Row>
                            <Col lg={12}>
                                <div className="text-center">
                                    <p className="mb-0">&copy; {new Date().getFullYear()} Velzon. Crafted with <i className="mdi mdi-heart text-danger"></i> by Themesbrand</p>
                                </div>
                            </Col>
                        </Row>
                    </Container>
                </footer>
            </div>
        </React.Fragment >
    );
};

export default CoverSuccessMsg;