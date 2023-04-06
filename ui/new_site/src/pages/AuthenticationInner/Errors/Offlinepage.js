import React from 'react';
import { Button, Card, CardBody, Col, Container, Row } from 'reactstrap';

// Import Images
// import offlineImg from "../../../assets/images/auth-offline.gif";

const Offlinepage = () => {
    document.title = "Offline Page | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <div className="auth-page-wrapper auth-bg-cover py-5 d-flex justify-content-center align-items-center min-vh-100">
                <div className="bg-overlay"></div>
                <div className="auth-page-content overflow-hidden pt-lg-5">
                    <Container>
                        <Row className="justify-content-center">
                            <Col xl={5}>
                                <Card className="overflow-hidden">
                                    <CardBody className="p-4">
                                        <div className="text-center">
                                            <img src={"https://img.themesbrand.com/velzon/images/auth-offline.gif"} alt="" height="210" />
                                            <h3 className="mt-4 fw-semibold">We're currently offline</h3>
                                            <p className="text-muted mb-4 fs-14">We can't show you this images because you aren't connected to the internet. When you’re back online refresh the page or hit the button below</p>
                                            <Button color="success" className="btn-border"
                                                // onClick={() => "window.location.href=window.location.href"}
                                            ><i className="ri-refresh-line align-bottom"></i> Refresh</Button>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
                        </Row>
                    </Container>
                </div>
            </div>
        </React.Fragment>
    );
};

export default Offlinepage;