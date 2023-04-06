import React from 'react';
import { Link } from 'react-router-dom';
import { Card, CardBody, Col, Container, Row, Button } from 'reactstrap';
import ParticlesAuth from "../ParticlesAuth";
//import images
import logoLight from "../../../assets/images/logo-light.png";

const BasicTwosVerify = () => {
    document.title = "Two Step Verification | Velzon - React Admin & Dashboard Template";

    const getInputElement = (index) => {
        return document.getElementById('digit' + index + '-input');
    }

    const moveToNext = (index) => {
        if (getInputElement(index).value.length === 1) {
            if (index !== 4) {
                getInputElement(index + 1).focus();
            } else {
                getInputElement(index).blur();
                // Submit code
                console.log('submit code');
            }
        }
    }

    return (
        <React.Fragment>
            <div className="auth-page-wrapper">
                <ParticlesAuth>
                    <div className="auth-page-content">
                        <Container>
                            <Row>
                                <Col lg={12}>
                                    <div className="text-center mt-sm-5 mb-4 text-white-50">
                                        <div>
                                            <Link to="/dashboard" className="d-inline-block auth-logo">
                                                <img src={logoLight} alt="" height="20" />
                                            </Link>
                                        </div>
                                        <p className="mt-3 fs-15 fw-medium">Premium Admin & Dashboard Template</p>
                                    </div>
                                </Col>
                            </Row>

                            <Row className="justify-content-center">
                                <Col md={8} lg={6} xl={5}>
                                    <Card className="mt-4">
                                        <CardBody className="p-4">
                                            <div className="mb-4">
                                                <div className="avatar-lg mx-auto">
                                                    <div className="avatar-title bg-light text-primary display-5 rounded-circle">
                                                        <i className="ri-mail-line"></i>
                                                    </div>
                                                </div>
                                            </div>

                                            <div className="p-2 mt-4">
                                                <div className="text-muted text-center mb-4 mx-lg-3">
                                                    <h4 className="">Verify Your Email</h4>
                                                    <p>Please enter the 4 digit code sent to <span className="fw-semibold">example@abc.com</span></p>
                                                </div>

                                                <form>
                                                    <Row>
                                                        <Col className="col-3">
                                                            <div className="mb-3">
                                                                <label htmlFor="digit1-input" className="visually-hidden">Digit 1</label>
                                                                <input type="text"
                                                                    className="form-control form-control-lg bg-light border-light text-center"
                                                                    maxLength="1"
                                                                    id="digit1-input" onKeyUp={() => moveToNext(1)} />
                                                            </div>
                                                        </Col>

                                                        <Col className="col-3">
                                                            <div className="mb-3">
                                                                <label htmlFor="digit2-input" className="visually-hidden">Digit 2</label>
                                                                <input type="text"
                                                                    className="form-control form-control-lg bg-light border-light text-center"
                                                                    maxLength="1"
                                                                    id="digit2-input" onKeyUp={() => moveToNext(2)} />
                                                            </div>
                                                        </Col>

                                                        <Col className="col-3">
                                                            <div className="mb-3">
                                                                <label htmlFor="digit3-input" className="visually-hidden">Digit 3</label>
                                                                <input type="text"
                                                                    className="form-control form-control-lg bg-light border-light text-center"
                                                                    maxLength="1"
                                                                    id="digit3-input" onKeyUp={() => moveToNext(3)} />
                                                            </div>
                                                        </Col>

                                                        <Col className="col-3">
                                                            <div className="mb-3">
                                                                <label htmlFor="digit4-input" className="visually-hidden">Digit 4</label>
                                                                <input type="text"
                                                                    className="form-control form-control-lg bg-light border-light text-center"
                                                                    maxLength="1"
                                                                    id="digit4-input" onKeyUp={() => moveToNext(4)} />
                                                            </div>
                                                        </Col>
                                                    </Row>
                                                </form>
                                                <div className="mt-3">
                                                    <Button color="success" className="w-100">Confirm</Button>
                                                </div>
                                            </div>
                                        </CardBody>
                                    </Card>
                                    <div className="mt-4 text-center">
                                        <p className="mb-0">Didn't receive a code ? <Link to="/auth-pass-reset-basic" className="fw-semibold text-primary text-decoration-underline">Resend</Link> </p>
                                    </div>
                                </Col>
                            </Row>
                        </Container>
                    </div>
                </ParticlesAuth >

            </div>
        </React.Fragment >
    );
};

export default BasicTwosVerify;