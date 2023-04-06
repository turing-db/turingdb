import React from 'react';
import { Col, Container, Row } from 'reactstrap';
import CountUp from "react-countup";

const Counter = () => {
    return (
        <React.Fragment>
            <section className="py-5 position-relative bg-light">
                <Container>
                    <Row className="text-center gy-4">
                        <Col lg={3} className="col-6">
                            <div>
                                <h2 className="mb-2"><span className="counter-value" data-target="100">
                                    <CountUp
                                        start={0}
                                        end={100}
                                        duration={3}
                                    />
                                </span>
                                    +
                                </h2>
                                <div className="text-muted">Projects Completed</div>
                            </div>
                        </Col>

                        <Col lg={3} className="col-6">
                            <div>
                                <h2 className="mb-2"><span className="counter-value" data-target="24">
                                    <CountUp
                                        start={0}
                                        end={24}
                                        duration={3}
                                    />
                                </span>
                                </h2>
                                <div className="text-muted">Win Awards</div>
                            </div>
                        </Col>

                        <Col lg={3} className="col-6">
                            <div>
                                <h2 className="mb-2"><span className="counter-value" data-target="20.3">
                                    <CountUp
                                        start={0}
                                        end={20.3}
                                        duration={3}
                                        decimal={1}
                                    />
                                </span>
                                    k
                                </h2>
                                <div className="text-muted">Satisfied Clients</div>
                            </div>
                        </Col>

                        <Col lg={3} className="col-6">
                            <div>
                                <h2 className="mb-2"><span className="counter-value" data-target="50">
                                    <CountUp
                                        start={0}
                                        end={50}
                                        duration={3}
                                    />
                                </span>
                                </h2>
                                <div className="text-muted">Employees</div>
                            </div>
                        </Col>
                    </Row>
                </Container>
            </section>
        </React.Fragment>
    );
};

export default Counter;