import React, { useState } from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import Cleave from "cleave.js/react";
import "cleave.js/dist/addons/cleave-phone.in";


const Masks = () => {
  document.title="Input Masks | Velzon - React Admin & Dashboard Template";

  const [date, setDate] = useState("");
  const [dateFormat, setDateFormat] = useState("");
  const [time, setTime] = useState("");
  const [timeFormat, setTimeFormat] = useState("");
  const [creditCardNo, setCreditCardNo] = useState("");
  const [delimiter, setDelimiter] = useState("");
  const [delimiter2, setDelimiter2] = useState("");
  const [prefix, setPrefix] = useState("");
  const [phone, setPhone] = useState("");
  const [number, setNumber] = useState("");

  //Date 
  function onDateChange(e) {
    setDate(e.target.rawValue);
  }
  //Date Format
  function onDateFormatChange(e) {
    setDateFormat(e.target.rawValue);
  }
  //Time 
  function onTimeChange(e) {
    setTime(e.target.rawValue);
  }
  //Time Format
  function onTimeFormatChange(e) {
    setTimeFormat(e.target.rawValue);
  }
  //Credit card 
  function onCreditCardChange(e) {
    setCreditCardNo(e.target.rawValue);
  }

  //Delimeter
  function onDelimiterChange(e) {
    setDelimiter(e.target.rawValue);
  }
  //Delimeter
  function onDelimiterChange2(e) {
    setDelimiter2(e.target.rawValue);
  }

  //Prefix
  function onPrefixChange(e) {
    setPrefix(e.target.rawValue);
  }
  //Phone
  function onPhoneChange(e) {
    setPhone(e.target.rawValue);
  }
  //Number
  function onNumberChange(e) {
    setNumber(e.target.rawValue);
  }

  return (
    <React.Fragment>
      <div className="page-content">       
        <Container fluid>
          <BreadCrumb title="Input Masks" pageTitle="Forms" />
          <Row>
            <Col lg={12}>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Examples</h4>
                </CardHeader>

                <CardBody >
                  <form action="#">
                    <div>
                      <h5 className="fs-14 mb-3 text-muted">Date Formatting</h5>
                      <Row >
                        <Col xl={6}>
                          <div className="mb-3">
                            <label htmlFor="cleave-date" className="form-label">Date</label>
                            <Cleave
                              placeholder="DD-MM-YYYY"
                              options={{
                                date: true,
                                delimiter: '-',
                                datePattern: ['d', 'm', 'Y']
                              }}
                              value={date}
                              onChange={e => onDateChange(e)}
                              className="form-control"
                            />
                          </div>
                        </Col>

                        <Col xl={6}>
                          <div className="mb-3">
                            <label htmlFor="cleave-date-format" className="form-label">Date Formatting</label>
                            <Cleave
                              placeholder="MM/YY"
                              options={{
                                date: true,
                                datePattern: ['m', 'y']
                              }}
                              value={dateFormat}
                              onChange={e => onDateFormatChange(e)}
                              className="form-control"
                            />
                          </div>
                        </Col>
                      </Row>
                    </div>

                    <div className="border mt-3 border-dashed"></div>

                    <div className="mt-4">
                      <h6 className="mb-3 fs-14 text-muted">Time Formatting</h6>
                      <Row>
                        <Col xl={6}>
                          <div className="mb-3">
                            <label htmlFor="cleave-time" className="form-label">Time</label>
                            <Cleave
                              placeholder="hh:mm:ss"
                              options={{
                                time: true,
                                timePattern: ['h', 'm', 's']
                              }}
                              value={time}
                              onChange={e => onTimeChange(e)}
                              className="form-control"
                            />
                          </div>

                        </Col>

                        <Col xl={6}>
                          <div className="mb-3">
                            <label htmlFor="cleave-time-format" className="form-label">Time Formatting</label>
                            <Cleave
                              placeholder="hh:mm"
                              options={{
                                time: true,
                                timePattern: ['h', 'm']
                              }}
                              value={timeFormat}
                              onChange={e => onTimeFormatChange(e)}
                              className="form-control"
                            />
                          </div>
                        </Col>
                      </Row>         
                    </div>

                    <div className="border mt-3 border-dashed"></div>

                    <div className="mt-4">
                      <h5 className="fs-14 mb-3 text-muted">Custom Options</h5>
                      <Row>
                        <Col xl={6}>
                          <div className="mb-3">
                            <label htmlFor="cleave-ccard" className="form-label">Credit Card</label>
                            <Cleave
                              placeholder="xxxx xxxx xxxx xxxx"
                              options={{
                                creditCard: true,
                              }}
                              value={creditCardNo}
                              onChange={e => onCreditCardChange(e)}
                              className="form-control"
                            />
                          </div>

                        </Col>

                        <Col xl={6}>
                          <div className="mb-3">
                            <label htmlFor="cleave-delimiter" className="form-label">Delimiter</label>
                            <Cleave
                              placeholder="xxx·xxx·xxx"
                              options={{
                                delimiter: '·',
                                blocks: [3, 3, 3],
                                uppercase: true
                              }}
                              value={delimiter}
                              onChange={e => onDelimiterChange(e)}
                              className="form-control"
                            />
                          </div>
                        </Col>
                      </Row>

                      <Row>
                        <Col xl={6}>
                          <div className="mb-3">
                            <label htmlFor="cleave-delimiters" className="form-label">Delimiters</label>
                            <Cleave
                              placeholder="xxx.xxx.xxx-xx"
                              options={{
                                delimiters: ['.', '.', '-'],
                                blocks: [3, 3, 3, 2],
                                uppercase: true
                              }}
                              value={delimiter2}
                              onChange={e => onDelimiterChange2(e)}
                              className="form-control"
                            />
                          </div>
                        </Col>

                        <Col xl={6}>
                          <div className="mb-3">
                            <label htmlFor="cleave-prefix" className="form-label">Prefix</label>
                            <Cleave
                              options={{
                                prefix: 'PREFIX',
                                delimiter: '-',
                                blocks: [6, 4, 4, 4],
                                uppercase: true
                              }}
                              value={prefix}
                              onChange={e => onPrefixChange(e)}
                              className="form-control"
                            />
                          </div>
                        </Col>
                      </Row>

                      <Row>
                        <Col xl={6}>
                          <div className="mb-3 mb-xl-0">
                            <label htmlFor="cleave-phone" className="form-label">Phone</label>
                            <Cleave
                              placeholder="xxxx xxx xxx"
                              options={{
                                phone: true,
                                phoneRegionCode: "IN"
                              }}
                              value={phone}
                              onChange={onPhoneChange}
                              className="form-control"
                            />
                          </div>
                        </Col>

                        <Col xl={6}>
                          <div className="mb-0">
                            <label htmlFor="cleave-numeral" className="form-label">Numeral Formatting</label>
                            <Cleave
                              placeholder="Enter numeral"
                              options={{
                                numeral: true,
                                numeralThousandsGroupStyle: 'thousand'
                              }}
                              value={number}
                              onChange={e => onNumberChange(e)}
                              className="form-control"
                            />
                          </div>
                        </Col>
                      </Row>
                    </div>
                  </form>
                </CardBody>
              </Card>
            </Col>
          </Row>
        </Container>
      </div>
    </React.Fragment>
  );
};

export default Masks;
