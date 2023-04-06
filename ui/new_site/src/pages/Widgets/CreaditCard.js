import React, { useState } from 'react';
import { Card, CardBody, Col, Row } from 'reactstrap';
import Cards from 'react-credit-cards';
import 'react-credit-cards/es/styles-compiled.css';
import 'react-credit-cards/lib/styles.scss';
import Cleave from "cleave.js/react";
import "cleave.js/dist/addons/cleave-phone.in";

const CreditCard = () => {

    const [state, setState] = useState({
        cardNumber: "",
        cardHolder: "",
        expiry: "",
        cvc: "",
        focus: "",
    })

    const onChangeHandler = (e, name) => {
        if (name === "cardNumber") {
            setState({ ...state, cardNumber: e.target.value })
        }
        else if (name === "cardHolder") {
            setState({ ...state, cardHolder: e.target.value })
        }
        else if (name === "expiry") {
            setState({ ...state, expiry: e.target.value })
        }
        else if (name === "cvc") {
            setState({ ...state, cvc: e.target.value })
        }
    }

    const handleInputFocus = (e) => {
        setState({ ...state, focus: e.target.name })
    }

    return (
        <React.Fragment>
            <Row>
                <Col xs={12}>
                    <h5 className="text-decoration-underline mb-3 mt-2 pb-3">Credit Card</h5>
                </Col>
            </Row>

            <Row>
                <Col xxl={4}>
                    <Card className="card-height-100">
                        <div className="card-header">
                            <h5 className="card-title mb-0">Credit Card - Custom</h5>
                        </div>
                        <CardBody>
                            <div className="mx-auto" style={{ maxWidth: "350px" }}>
                                <div className="text-bg-info bg-gradient p-4 rounded-3 mb-3">
                                    <div className="d-flex">
                                        <div className="flex-grow-1">
                                            <i className="bx bx-chip h1 text-warning"></i>
                                        </div>
                                        <div className="flex-shrink-0">
                                            <i className="bx bxl-visa display-2 mt-n3"></i>
                                        </div>
                                    </div>
                                    <div className="card-number fs-20" id="card-num-elem">
                                        XXXX XXXX XXXX XXXX
                                    </div>

                                    <Row className="mt-4">
                                        <Col xs={4}>
                                            <div>
                                                <div className="text-white-50">Card Holder</div>
                                                <div id="card-holder-elem" className="fw-medium fs-14">Full Name</div>
                                            </div>
                                        </Col>
                                        <Col xs={4}>
                                            <div className="expiry">
                                                <div className="text-white-50">Expires</div>
                                                <div className="fw-medium fs-14">
                                                    <span id="expiry-month-elem">00</span>
                                                    /
                                                    <span id="expiry-year-elem">0000</span>
                                                </div>
                                            </div>
                                        </Col>
                                        <Col xs={4}>
                                            <div>
                                                <div className="text-white-50">CVC</div>
                                                <div id="cvc-elem" className="fw-medium fs-14">---</div>
                                            </div>
                                        </Col>
                                    </Row>
                                </div>

                            </div>


                            <form id="custom-card-form" autoComplete="off">
                                <div className="mb-3">
                                    <label htmlFor="card-num-input" className="form-label">Card Number</label>
                                    <input id="card-num-input" className="form-control" maxLength="19" placeholder="0000 0000 0000 0000" />
                                </div>

                                <div className="mb-3">
                                    <label htmlFor="card-holder-input" className="form-label">Card Holder</label>
                                    <input type="text" className="form-control" id="card-holder-input" placeholder="Enter holder name" />
                                </div>

                                <Row>
                                    <div className="col-lg-4">
                                        <div>
                                            <label htmlFor="expiry-month-input" className="form-label">Expiry Month</label>
                                            <select className="form-select" id="expiry-month-input">
                                                <option></option>
                                                <option value="01">01</option>
                                                <option value="02">02</option>
                                                <option value="03">03</option>
                                                <option value="04">04</option>
                                                <option value="05">05</option>
                                                <option value="06">06</option>
                                                <option value="07">07</option>
                                                <option value="08">08</option>
                                                <option value="09">09</option>
                                                <option value="10">10</option>
                                                <option value="11">11</option>
                                                <option value="12">12</option>
                                            </select>
                                        </div>
                                    </div>


                                    <div className="col-lg-4">
                                        <div>
                                            <label htmlFor="expiry-year-input" className="form-label">Expiry Year</label>
                                            <select className="form-select" id="expiry-year-input">
                                                <option></option>
                                                <option value="2020">2020</option>
                                                <option value="2021">2021</option>
                                                <option value="2022">2022</option>
                                                <option value="2023">2023</option>
                                                <option value="2024">2024</option>
                                                <option value="2025">2025</option>
                                                <option value="2026">2026</option>
                                                <option value="2027">2027</option>
                                                <option value="2028">2028</option>
                                                <option value="2029">2029</option>
                                                <option value="2030">2030</option>
                                            </select>
                                        </div>
                                    </div>

                                    <div className="col-lg-4">
                                        <div className="cvc">
                                            <label htmlFor="cvc-input" className="form-label">CVC</label>
                                            <input type="text" id="cvc-input" className="form-control" maxLength="3" />
                                        </div>
                                    </div>
                                </Row>
                                <button className="btn btn-danger w-100 mt-3" type="submit">Pay Now</button>
                            </form>

                        </CardBody>
                    </Card>

                </Col>

                <div className="col-xl-4">
                    <div className="card">
                        <div className="card-header">
                            <h5 className="card-title mb-0">Credit Card - card js plugin</h5>
                        </div>
                        <div className="card-body">
                            <div className="card-wrapper mb-3">

                                <Cards
                                    cvc={state.cvc}
                                    expiry={state.expiry}
                                    focused={state.focus}
                                    name={state.cardHolder}
                                    number={state.cardNumber}
                                />
                            </div>

                            <div className="form-container active">
                                <form action="" id="card-form-elem" autoComplete="off">
                                    <div className="mb-3">
                                        <label htmlFor="card-number-input" className="form-label">Card Number</label>
                                        <input maxLength="16" onFocus={(e) => handleInputFocus(e)} onChange={(e) => onChangeHandler(e, 'cardNumber')} name="cardNumber" className="form-control" placeholder="Card number" type="tel" id="card-number-input" />
                                    </div>
                                    <div className="mb-3">
                                        <label htmlFor="card-name-input" className="form-label">Card Holder</label>
                                        <input maxLength="25" onFocus={(e) => handleInputFocus(e)} onChange={(e) => onChangeHandler(e, 'cardHolder')} className="form-control" name='cardHolder' placeholder="Full name" type="text" id="card-name-input" />
                                    </div>
                                    <Row>
                                        <Col sm={6}>
                                            <div className="mb-3">
                                                <label htmlFor="card-expiry-input" className="form-label">Expiry</label>
                                                <Cleave
                                                    placeholder="MM/YY"
                                                    options={{
                                                        date: true,
                                                        datePattern: ['m', 'y']
                                                    }}
                                                    value={state.expiry}
                                                    name="cvc"
                                                    onChange={e => onChangeHandler(e, 'expiry')}
                                                    onFocus={(e) => handleInputFocus(e)}
                                                    className="form-control"
                                                    id="card-cvc-input"
                                                />
                                            </div>
                                        </Col>
                                        <Col sm={6}>
                                            <div className="mb-3">
                                                <label htmlFor="card-cvc-input" className="form-label">CVC</label>
                                                <input onFocus={(e) => handleInputFocus(e)} onChange={(e) => onChangeHandler(e, 'cvc')} name='cvc' className="form-control" placeholder="CVC" maxLength="3" type="text" id="card-cvc-input" />
                                            </div>
                                        </Col>
                                    </Row>
                                    <button className="btn btn-danger w-100" type="button">Pay Now</button>
                                </form>
                            </div>
                        </div>
                    </div>
                </div>

            </Row>
        </React.Fragment>
    );
}

export default CreditCard;