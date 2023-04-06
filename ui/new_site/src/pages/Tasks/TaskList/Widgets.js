import React from 'react';
import { Card, CardBody, Col } from 'reactstrap';
import CountUp from "react-countup";
import { taskWidgets } from '../../../common/data';

const Widgets = () => {
    return (
        <React.Fragment>
            {taskWidgets.map((item, key) => (
                <Col xxl={3} sm={6} key={key}>
                    <Card className="card-animate">
                        <CardBody>
                            <div className="d-flex justify-content-between">
                                <div>
                                    <p className="fw-medium text-muted mb-0">{item.label}</p>
                                    <h2 className="mt-4 ff-secondary fw-semibold">
                                        <span className="counter-value">
                                            <CountUp
                                                start={0}
                                                end={item.counter}
                                                decimal={item.decimals}
                                                suffix={item.suffix}
                                                duration={3}
                                            />
                                        </span>
                                    </h2>
                                    <p className="mb-0 text-muted"><span className={"badge bg-light mb-0 text-" + item.badgeClass}>
                                        <i className={"align-middle " + item.badge}></i> {item.percentage}
                                    </span> vs. previous month</p>
                                </div>
                                <div>
                                    <div className="avatar-sm flex-shrink-0">
                                        <span className={"avatar-title rounded-circle fs-4 bg-soft-" + item.iconClass + " text-" + item.iconClass}>
                                            <i className={item.icon}></i>
                                        </span>
                                    </div>
                                </div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>
            ))}
        </React.Fragment>
    );
};

export default Widgets;