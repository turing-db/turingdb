import React from 'react';
import CountUp from "react-countup";
import { Card, CardBody, Col } from 'reactstrap';
import { cyptoWidgets } from '../../common/data';

const Widgets = () => {
    return (
        <React.Fragment>
            {(cyptoWidgets || []).map((item, key) => (
                <Col lg={4} md={6} key={key}>
                    <Card>
                        <CardBody>
                            <div className="d-flex align-items-center">
                                <div className="avatar-sm flex-shrink-0">
                                    <span className="avatar-title bg-light text-primary rounded-circle fs-3">
                                        <i className={"align-middle " + item.icon}></i>
                                    </span>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <p className="text-uppercase fw-semibold fs-13 text-muted mb-1">{item.label}</p>
                                    <h4 className=" mb-0"> <CountUp start={0} end={item.counter} decimals={item.decimal} separator={item.separator} prefix={item.prefix} duration={3} />
                                    </h4>
                                </div>
                                <div className="flex-shrink-0 align-self-end">
                                    <span className={"badge badge-soft-" + item.badgeColor}><i className={"align-middle me-1 " + item.badge}></i>{item.percentage} %<span>
                                    </span></span></div>
                            </div>
                        </CardBody>
                    </Card>
                </Col>
            ))}
        </React.Fragment>
    );
};

export default Widgets;