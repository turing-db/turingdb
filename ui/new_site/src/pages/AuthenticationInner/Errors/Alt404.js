import React from 'react';
import { Link } from 'react-router-dom';
import { Card, CardBody, Col, Container, Row } from 'reactstrap';

const Alt404 = () => {
    document.title="404 Error Alt | Velzon - React Admin & Dashboard Template";
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
                                            <lord-icon className="avatar-xl"
                                                src="https://cdn.lordicon.com/spxnqpau.json"
                                                trigger="loop"
                                                colors="primary:#405189,secondary:#0ab39c"
                                                style={{ width: "120px", height: "120px" }}>
                                            </lord-icon>
                                            <h1 className="text-primary mb-4">Oops !</h1>
                                            <h4 className="text-uppercase">Sorry, Page not Found 😭</h4>
                                            <p className="text-muted mb-4">The page you are looking for not available!</p>
                                            <Link to="/dashboard" className="btn btn-success"><i className="mdi mdi-home me-1"></i>Back to home</Link>
                                        </div>
                                    </CardBody>
                                </Card>
                            </Col>
                        </Row>
                    </Container>
                </div>
            </div>
        </React.Fragment >
    );
};

export default Alt404;