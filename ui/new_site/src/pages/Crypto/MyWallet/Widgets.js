import React from 'react';
import { Card, CardBody } from 'reactstrap';

const Widgets = () => {
    return (
        <React.Fragment>
            <Card>
                <CardBody className="bg-soft-warning">
                    <div className="d-flex">
                        <div className="flex-grow-1">
                            <h5 className="fs-14 mb-3">My Portfolio</h5>
                            <h2>$61,91,967<small className="text-muted fs-14">.29</small></h2>
                            <p className="text-muted mb-0">$25,10,974 <small className="badge badge-soft-success"><i className="ri-arrow-right-up-line fs-13 align-bottom"></i>4.37%</small></p>
                        </div>
                        <div className="flex-shrink-0">
                            <i className="mdi mdi-wallet-outline text-primary h1"></i>
                        </div>
                    </div>
                </CardBody>
            </Card>
            <Card>
                <CardBody>
                    <div className="d-flex">
                        <div className="flex-grow-1">
                            <h5 className="fs-14 mb-3">Today's Profit</h5>
                            <h2>$2,74,365<small className="text-muted fs-14">.84</small></h2>
                            <p className="text-muted mb-0">$9,10,564 <small className="badge badge-soft-success"><i className="ri-arrow-right-up-line fs-13 align-bottom"></i>1.25%</small></p>
                        </div>
                        <div className="flex-shrink-0">
                            <i className="ri-hand-coin-line text-primary h1"></i>
                        </div>
                    </div>
                </CardBody>
            </Card>
            <Card>
                <CardBody>
                    <div className="d-flex">
                        <div className="flex-grow-1">
                            <h5 className="fs-14 mb-3">Overall Profit</h5>
                            <h2>$32,67,120<small className="text-muted fs-14">.42</small></h2>
                            <p className="text-muted mb-0">$18,22,730 <small className="badge badge-soft-success"><i className="ri-arrow-right-up-line fs-13 align-bottom"></i>8.34%</small></p>
                        </div>
                        <div className="flex-shrink-0">
                            <i className="ri-line-chart-line text-primary h1"></i>
                        </div>
                    </div>
                </CardBody>
            </Card>
        </React.Fragment>
    );
};

export default Widgets;