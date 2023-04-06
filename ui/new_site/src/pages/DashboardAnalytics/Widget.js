import React from 'react';
import { Card, CardBody, Col, Row } from 'reactstrap';
import CountUp from "react-countup";

//Import Icons
import FeatherIcon from "feather-icons-react";

const Widget = () => {
    return (
        <React.Fragment>
            <Row>
                <Col md={6}>
                    <Card className="card-animate">
                        <CardBody>
                            <div className="d-flex justify-content-between">
                                <div>
                                    <p className="fw-medium text-muted mb-0">Users</p>
                                    <h2 className="mt-4 ff-secondary fw-semibold">
                                        <span className="counter-value">
                                            <CountUp
                                                start={0}
                                                end={28.05}
                                                decimals={1}
                                                duration={4}
                                            />
                                            0</span>k</h2>
                                    <p className="mb-0 text-muted"><span className="badge bg-light text-success mb-0">
                                        <i className="ri-arrow-up-line align-middle"></i> 16.24 %
                                    </span> vs. previous month</p>
                                </div>
                                <div>
                                    <div className="avatar-sm flex-shrink-0">
                                        <span className="avatar-title bg-soft-info rounded-circle fs-2">
                                            <FeatherIcon
                                                icon="users"
                                                className="text-info"
                                            />
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>

                <Col md={6}>
                    <Card className="card-animate">
                        <CardBody>
                            <div className="d-flex justify-content-between">
                                <div>
                                    <p className="fw-medium text-muted mb-0">Sessions</p>
                                    <h2 className="mt-4 ff-secondary fw-semibold">
                                        <span className="counter-value" data-target="97.66">
                                            <CountUp
                                                start={0}
                                                end={97.66}
                                                decimals={2}
                                                duration={4}
                                            />
                                        </span>k</h2>
                                    <p className="mb-0 text-muted"><span className="badge bg-light text-danger mb-0">
                                        <i className="ri-arrow-down-line align-middle"></i> 3.96 %
                                    </span> vs. previous month</p>
                                </div>
                                <div>
                                    <div className="avatar-sm flex-shrink-0">
                                        <span className="avatar-title bg-soft-info rounded-circle fs-2">
                                            <FeatherIcon
                                                icon="activity"
                                                className="text-info"
                                            />
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>
            </Row>

            <Row>
                <Col md={6}>
                    <Card className="card-animate">
                        <CardBody>
                            <div className="d-flex justify-content-between">
                                <div>
                                    <p className="fw-medium text-muted mb-0">Avg. Visit Duration</p>
                                    <h2 className="mt-4 ff-secondary fw-semibold">
                                        <span className="counter-value" data-target="3">
                                            <CountUp
                                                start={0}
                                                end={3}
                                                duration={4}
                                            />
                                        </span>m{" "}
                                        <span className="counter-value" data-target="40">
                                            <CountUp
                                                start={0}
                                                end={40}
                                                duration={4}
                                            />
                                        </span>sec</h2>
                                    <p className="mb-0 text-muted"><span className="badge bg-light text-danger mb-0">
                                        <i className="ri-arrow-down-line align-middle"></i> 0.24 %
                                    </span> vs. previous month</p>
                                </div>
                                <div>
                                    <div className="avatar-sm flex-shrink-0">
                                        <span className="avatar-title bg-soft-info rounded-circle fs-2">
                                            <FeatherIcon
                                                icon="clock"
                                                className="text-info"
                                            />
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>

                <Col md={6}>
                    <Card className="card-animate">
                        <CardBody>
                            <div className="d-flex justify-content-between">
                                <div>
                                    <p className="fw-medium text-muted mb-0">Bounce Rate</p>
                                    <h2 className="mt-4 ff-secondary fw-semibold">
                                        <span className="counter-value" data-target="33.48">
                                            <CountUp
                                                start={0}
                                                end={33.48}
                                                decimals={2}
                                                duration={4}
                                            />
                                        </span>%</h2>
                                    <p className="mb-0 text-muted"><span className="badge bg-light text-success mb-0">
                                        <i className="ri-arrow-up-line align-middle"></i> 7.05 %
                                    </span> vs. previous month</p>
                                </div>
                                <div>
                                    <div className="avatar-sm flex-shrink-0">
                                        <span className="avatar-title bg-soft-info rounded-circle fs-2">
                                            <FeatherIcon
                                                icon="external-link"
                                                className="text-info"
                                            />
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>
            </Row>
        </React.Fragment>
    );
};

export default Widget;