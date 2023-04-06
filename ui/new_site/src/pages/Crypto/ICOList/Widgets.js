import React from 'react';
import { Card, CardBody, Col } from 'reactstrap';
import { CryptoicoWidgets } from '../../../common/data';

const Widgets = () => {
    return (
        <React.Fragment>
            {(CryptoicoWidgets || []).map((item, key) => (
                <Col key={key}>
                    <Card>
                        <CardBody className="d-flex">
                            <div className="flex-grow-1">
                                <h4>{item.counter}</h4>
                                <h6 className="text-muted mb-0">{item.label}</h6>
                            </div>
                            <div className="flex-shrink-0 avatar-sm">
                                <div className={"avatar-title bg-soft-" + item.iconClass + " text-" + item.iconClass + " fs-22 rounded"}>
                                    <i className={item.icon}></i>
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