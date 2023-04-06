import React from 'react';
import { Link } from 'react-router-dom';
import { Button, Card, Col, Container, Row } from 'reactstrap';


//import images
import avatar1 from "../../../assets/images/users/avatar-1.jpg";

import AuthSlider from '../authCarousel';

const CoverLockScreen = () => {
    document.title="Lock Screen | Velzon - React Admin & Dashboard Template";
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
                                                <div className="p-lg-5 p-4">
                                                    <div>
                                                        <h5 className="text-primary">Lock Screen</h5>
                                                        <p className="text-muted">Enter your password to unlock the screen!</p>
                                                    </div>
                                                    <div className="user-thumb text-center">
                                                        <img src={avatar1} className="rounded-circle img-thumbnail avatar-lg" alt="thumbnail" />
                                                        <h5 className="font-size-15 mt-3">Anna Adame</h5>
                                                    </div>

                                                    <div className="mt-4">
                                                        <form>
                                                            <div className="mb-3">
                                                                <label className="form-label" htmlFor="userpassword">Password</label>
                                                                <input type="password" className="form-control" id="userpassword" placeholder="Enter password" required />
                                                            </div>
                                                            <div className="mb-2 mt-4">
                                                                <Button color="success" className="w-100" type="submit">Unlock</Button>
                                                            </div>
                                                        </form>
                                                    </div>

                                                    <div className="mt-5 text-center">
                                                        <p className="mb-0">Not you ? return <Link to="/auth-signin-cover" className="fw-semibold text-primary text-decoration-underline"> Signin</Link></p>
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
        </React.Fragment>
    );
};

export default CoverLockScreen;