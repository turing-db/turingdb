import React from 'react';
import { Link } from 'react-router-dom';
import { Card, Col, Container, Row } from 'reactstrap';

import AuthSlider from '../authCarousel';

const CoverLogout = () => {
    document.title="Log Out | Velzon - React Admin & Dashboard Template";
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
                                                <lord-icon
                                                    src="https://cdn.lordicon.com/hzomhqxz.json"
                                                    trigger="loop"
                                                    colors="primary:#405189,secondary:#08a88a"
                                                    style={{ width: "180px", height: "180px" }}>
                                                </lord-icon>

                                                <div className="mt-4 pt-2">
                                                    <h5>You are Logged Out</h5>
                                                    <p className="text-muted">Thank you for using <span className="fw-semibold">velzon</span> admin template</p>
                                                    <div className="mt-4">
                                                        <Link to="/auth-signin-basic" className="btn btn-success w-100">Sign In</Link>
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

                {/* <!-- end Footer --> */}
            </div>
        </React.Fragment>
    );
};

export default CoverLogout;