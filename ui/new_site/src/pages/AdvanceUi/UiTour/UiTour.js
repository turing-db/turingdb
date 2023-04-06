import React, { useContext, useEffect } from 'react';
import { Button, Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';

// import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

// Import Images
import logodark from "../../../assets/images/logo-dark.png";
import logolight from "../../../assets/images/logo-light.png";

import { ShepherdTour, ShepherdTourContext } from 'react-shepherd';
import newSteps from './steps';

const tourOptions = {
    defaultStepOptions: {
        cancelIcon: {
            enabled: true
        },
        classes: 'shadow-md bg-purple-dark',
        scrollTo: { behavior: "smooth", block: "center" },
    },
    useModalOverlay: true
};

function Autton() {
    const tour = useContext(ShepherdTourContext);
    useEffect(() => {
        tour.start();
    }, [tour]);
    return (<> </>);
}

const UiTour = () => {
    document.title="Tour | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="page-content">
                
                <Container fluid>
                    <ShepherdTour steps={newSteps} tourOptions={tourOptions}>
                        <Autton />
                        <BreadCrumb title="Tour" pageTitle="Advance UI" />
                        <Row>
                            <Col lg={12}>
                                <Card>
                                    <CardHeader>
                                        <h4 className="card-title mb-0">Shepherdjs</h4>
                                    </CardHeader>

                                    <CardBody>

                                        <p className="text-muted mb-4">Use when you want to help users along getting accustomed to your user interface or
                                            Use when you want to notify your users of new or unfamiliar features.
                                        </p>

                                        <div>
                                            <Row className="justify-content-center">
                                                <Col lg={5}>
                                                    <div className="text-center mt-4 mb-5">
                                                        <div className="py-3 px-2 d-inline-block" id="logo-tour">
                                                            <img src={logodark} className="card-logo card-logo-dark" alt="logo" height="17" />
                                                            <img src={logolight} className="card-logo card-logo-light" alt="logo" height="17" />
                                                        </div>
                                                        <h5 className="fs-16">Responsive Admin Dashboard Template</h5>
                                                        <p className="text-muted">Vestibulum auctor tincidunt semper. Phasellus
                                                            ut vulputate lacus. Suspendisse ultricies mi eros, sit amet
                                                            tempor nulla varius sed dapibus in tellus.</p>
                                                        <div className="hstack gap-2 justify-content-center">
                                                            <Button color="primary" size="sm">View more</Button>
                                                            <Button color="success" size="sm">Email us</Button>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>

                                            <Row className="justify-content-center">
                                                <Col xxl={9} lg={10} sm={8}>
                                                    <Row>
                                                        <Col lg={4}>
                                                            <Card className="border shadow-none">
                                                                <CardBody className="text-center">
                                                                    <div className="avatar-md mx-auto mb-4" id="register-tour">
                                                                        <div className="avatar-title bg-light rounded-circle text-primary fs-24">
                                                                            <i className="ri-edit-box-line"></i>
                                                                        </div>
                                                                    </div>
                                                                    <h5>Free register</h5>
                                                                    <p className="text-muted mb-0">Get your Free Velzon account now.</p>
                                                                </CardBody>
                                                            </Card>
                                                        </Col>

                                                        <Col lg={4}>
                                                            <Card className="border shadow-none">
                                                                <CardBody className="text-center">
                                                                    <div className="avatar-md mx-auto mb-4" id="login-tour">
                                                                        <div
                                                                            className="avatar-title bg-light rounded-circle text-primary fs-24">
                                                                            <i className="ri-user-shared-line"></i>
                                                                        </div>
                                                                    </div>
                                                                    <h5>Log in account</h5>
                                                                    <p className="text-muted mb-0">Sign in to continue to Velzon.</p>
                                                                </CardBody>
                                                            </Card>
                                                        </Col>

                                                        <Col lg={4}>
                                                            <Card className="border shadow-none">
                                                                <CardBody className="text-center">
                                                                    <div className="avatar-md mx-auto mb-4" id="getproduct-tour">
                                                                        <div
                                                                            className="avatar-title bg-light rounded-circle text-primary fs-24">
                                                                            <i className="ri-file-download-line"></i>
                                                                        </div>
                                                                    </div>
                                                                    <h5>Get Product</h5>
                                                                    <p className="text-muted mb-0">Sign in to continue to Velzon.</p>
                                                                </CardBody>
                                                            </Card>
                                                        </Col>
                                                    </Row>
                                                </Col>
                                            </Row>
                                            <div className="text-center my-4">
                                                <Button color="success" id="thankyou-tour">Thank you !</Button>
                                            </div>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
                        </Row>
                    </ShepherdTour>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default UiTour;
