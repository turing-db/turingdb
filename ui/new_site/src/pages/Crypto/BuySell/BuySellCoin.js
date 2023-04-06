import React, { useState } from 'react';
import { Card, CardBody, CardHeader, Col, Input, Label, Nav, NavItem, NavLink, Row, TabContent, TabPane } from 'reactstrap';
import classnames from 'classnames';

const BuySellCoin = () => {
    //Tab 
    const [activeTab, setActiveTab] = useState('1');
    const toggleTab = (tab) => {
        if (activeTab !== tab) {
            setActiveTab(tab);
        }
    };
    return (
        <React.Fragment>
            <Col xxl={3}>
                <Card className="card-height-100">
                    <CardHeader>
                        <Nav className="nav-tabs-custom rounded card-header-tabs nav-justified border-bottom-0 mx-n3" role="tablist">
                            <NavItem>
                                <NavLink
                                    href="#"
                                    className={classnames({ active: activeTab === '1' })}
                                    onClick={() => { toggleTab('1'); }}
                                >
                                    Buy</NavLink>
                            </NavItem>
                            <NavItem>
                                <NavLink
                                    href="#"
                                    className={classnames({ active: activeTab === '2' })}
                                    onClick={() => { toggleTab('2'); }}
                                >
                                    Sell</NavLink>
                            </NavItem>
                        </Nav>
                    </CardHeader>
                    <CardBody className="p-0">
                        <TabContent activeTab={activeTab} className="text-muted">
                            <TabPane tabId="1">
                                <div className="p-3 bg-soft-warning">
                                    <div className="float-end ms-2">
                                        <h6 className="text-warning mb-0">USD Balance : <span className="text-dark">$12,426.07</span></h6>
                                    </div>
                                    <h6 className="mb-0 text-danger">Buy Coin</h6>
                                </div>
                                <div className="p-3">
                                    <Row>
                                        <Col xs-={6}>
                                            <div className="mb-3">
                                                <Label>Currency :</Label>
                                                <select className="form-select">
                                                    <option>BTC</option>
                                                    <option>ETH</option>
                                                    <option>LTC</option>
                                                </select>
                                            </div>
                                        </Col>
                                        <Col xs-={6}>
                                            <div className="mb-3">
                                                <Label>Payment Method :</Label>
                                                <select className="form-select">
                                                    <option>Wallet Balance</option>
                                                    <option>Credit / Debit Card</option>
                                                    <option>PayPal</option>
                                                    <option>Payoneer</option>
                                                </select>
                                            </div>
                                        </Col>
                                    </Row>
                                    <div>
                                        <div className="input-group mb-3">
                                            <Label className="input-group-text">Amount</Label>
                                            <Input type="text" className="form-control" placeholder="0" />
                                        </div>

                                        <div className="input-group mb-3">
                                            <Label className="input-group-text">Price</Label>
                                            <Input type="text" className="form-control" placeholder="2.045585" />
                                            <Label className="input-group-text">$</Label>
                                        </div>

                                        <div className="input-group mb-0">
                                            <Label className="input-group-text">Total</Label>
                                            <Input type="text" className="form-control" placeholder="2700.16" />
                                        </div>
                                    </div>
                                    <div className="mt-3 pt-2">
                                        <div className="d-flex mb-2">
                                            <div className="flex-grow-1">
                                                <p className="mb-0">Transaction Fees<span className="text-muted ms-1 fs-11">(0.05%)</span></p>
                                            </div>
                                            <div className="flex-shrink-0">
                                                <h6 className="mb-0">$1.08</h6>
                                            </div>
                                        </div>
                                        <div className="d-flex mb-2">
                                            <div className="flex-grow-1">
                                                <p className="mb-0">Minimum Received<span className="text-muted ms-1 fs-11">(2%)</span></p>
                                            </div>
                                            <div className="flex-shrink-0">
                                                <h6 className="mb-0">$7.85</h6>
                                            </div>
                                        </div>
                                        <div className="d-flex">
                                            <div className="flex-grow-1">
                                                <p className="mb-0">Estimated Rate</p>
                                            </div>
                                            <div className="flex-shrink-0">
                                                <h6 className="mb-0">1 BTC ~ $46982.70</h6>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="mt-3 pt-2">
                                        <button type="button" className="btn btn-primary w-100">Buy Coin</button>
                                    </div>
                                </div>
                            </TabPane>

                            <TabPane tabId="2">
                                <div className="p-3 bg-soft-warning">
                                    <div className="float-end ms-2">
                                        <h6 className="text-warning mb-0">USD Balance : <span className="text-dark">$12,426.07</span></h6>
                                    </div>
                                    <h6 className="mb-0 text-danger">Sell Coin</h6>
                                </div>
                                <div className="p-3">
                                    <Row>
                                        <Col xs-={6}>
                                            <div className="mb-3">
                                                <Label>Currency :</Label>
                                                <select className="form-select">
                                                    <option>BTC</option>
                                                    <option>ETH</option>
                                                    <option>LTC</option>
                                                </select>
                                            </div>
                                        </Col>
                                        <Col xs-={6}>
                                            <div className="mb-3">
                                                <Label>Email :</Label>
                                                <Input type="email" className="form-control" placeholder="example@email.com" />
                                            </div>
                                        </Col>
                                    </Row>
                                    <div>
                                        <div className="input-group mb-3">
                                            <Label className="input-group-text">Amount</Label>
                                            <Input type="text" className="form-control" placeholder="0" />
                                        </div>
                                        <div className="input-group mb-3">
                                            <Label className="input-group-text">Price</Label>
                                            <Input type="text" className="form-control" placeholder="2.045585" />
                                            <Label className="input-group-text">$</Label>
                                        </div>
                                        <div className="input-group mb-0">
                                            <Label className="input-group-text">Total</Label>
                                            <Input type="text" className="form-control" placeholder="2700.16" />
                                        </div>
                                    </div>
                                    <div className="mt-3 pt-2">
                                        <div className="d-flex mb-2">
                                            <div className="flex-grow-1">
                                                <p className="fs-13 mb-0">Transaction Fees<span className="text-muted ms-1 fs-12">(0.05%)</span></p>
                                            </div>
                                            <div className="flex-shrink-0">
                                                <h6 className="mb-0">$1.08</h6>
                                            </div>
                                        </div>
                                        <div className="d-flex mb-2">
                                            <div className="flex-grow-1">
                                                <p className="fs-13 mb-0">Minimum Received<span className="text-muted ms-1 fs-12">(2%)</span></p>
                                            </div>
                                            <div className="flex-shrink-0">
                                                <h6 className="mb-0">$7.85</h6>
                                            </div>
                                        </div>
                                        <div className="d-flex">
                                            <div className="flex-grow-1">
                                                <p className="fs-13 mb-0">Estimated Rate</p>
                                            </div>
                                            <div className="flex-shrink-0">
                                                <h6 className="mb-0">1 BTC ~ $34572.00</h6>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="mt-3 pt-2">
                                        <button type="button" className="btn btn-danger w-100">Sell Coin</button>
                                    </div>
                                </div>
                            </TabPane>
                        </TabContent>
                    </CardBody>
                </Card>
            </Col>
        </React.Fragment>
    );
};

export default BuySellCoin;