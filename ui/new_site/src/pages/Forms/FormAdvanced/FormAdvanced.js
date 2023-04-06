import React, { useState } from "react";
import {
  Card,
  CardBody,
  CardHeader,
  Col,
  Container,
  Input,
  Row,
} from "reactstrap";

import BreadCrumb from "../../../Components/Common/BreadCrumb";

import DualListbox from './DualListbox';

const FormAdvanced = () => {
  document.title="File Advanced | Velzon - React Admin & Dashboard Template";

  const [defaultCounter, setdefaultCounter] = useState(5);
  const [lightCounter, setlightCounter] = useState(5);
  const [fullwidthCounter, setfullwidthCounter] = useState(5);
  const [lfullwidthCounter, setlfullwidthCounter] = useState(5);
  const [blueCounter, setblueCounter] = useState(5);
  const [skyCounter, setskyCounter] = useState(5);
  const [greenCounter, setgreenCounter] = useState(5);
  const [tealCounter, settealCounter] = useState(5);
  const [yellowCounter, setyellowCounter] = useState(5);
  const [redCounter, setredCounter] = useState(5);

  function countUP(id, prev_data_attr) {
    id(prev_data_attr + 1);
  }

  function countDown(id, prev_data_attr) {
    id(prev_data_attr - 1);
  }

  return (
    <React.Fragment>
      <div className="page-content">        
        <Container fluid>
          <BreadCrumb title="Form Advanced" pageTitle="Forms" />
          <Row>
            <Col lg={12}>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Form Input Spin</h4>
                </CardHeader>

                <CardBody>
                  <div>
                    <Row className="gy-4">
                      <Col sm={6}>
                        <div>
                          <h5 className="fs-13 fw-medium text-muted">
                            Default
                          </h5>
                          <div className="input-step">
                            <button
                              type="button"
                              className="minus"
                              onClick={() => {
                                countDown(setdefaultCounter, defaultCounter);
                              }}
                            >
                              –
                            </button>
                            <Input
                              type="number"
                              className="product-quantity"
                              value={defaultCounter}
                              min="0"
                              max="100"
                              readOnly
                            />
                            <button
                              type="button"
                              className="plus"
                              onClick={() => {
                                countUP(setdefaultCounter, defaultCounter);
                              }}
                            >
                              +
                            </button>
                          </div>
                        </div>
                      </Col>

                      <Col sm={6}>
                        <div>
                          <h5 className="fs-13 fw-medium text-muted">Light</h5>
                          <div className="input-step light">
                            <button
                              type="button"
                              className="minus"
                              onClick={() => {
                                countDown(setlightCounter, lightCounter);
                              }}
                            >
                              –
                            </button>
                            <Input
                              type="number"
                              className="product-quantity"
                              value={lightCounter}
                              min="0"
                              max="100"
                              readOnly
                            />
                            <button
                              type="button"
                              className="plus"
                              onClick={() => {
                                countUP(setlightCounter, lightCounter);
                              }}
                            >
                              +
                            </button>
                          </div>
                        </div>
                      </Col>
                    </Row>

                    <div className="mt-4 pt-2">
                      <Row className="gy-4">
                        <Col sm={6}>
                          <div>
                            <h5 className="fs-13 fw-medium text-muted">
                              Default (Full width)
                            </h5>
                            <div className="input-step full-width">
                              <button
                                type="button"
                                className="minus"
                                onClick={() => {
                                  countDown(
                                    setfullwidthCounter,
                                    fullwidthCounter
                                  );
                                }}
                              >
                                –
                              </button>
                              <Input
                                type="number"
                                className="product-quantity"
                                value={fullwidthCounter}
                                min="0"
                                max="100"
                                readOnly
                              />
                              <button
                                type="button"
                                className="plus"
                                onClick={() => {
                                  countUP(
                                    setfullwidthCounter,
                                    fullwidthCounter
                                  );
                                }}
                              >
                                +
                              </button>
                            </div>
                          </div>
                        </Col>

                        <Col sm={6}>
                          <div>
                            <h5 className="fs-13 fw-medium text-muted">
                              Light (Full width)
                            </h5>
                            <div className="input-step full-width light">
                              <button
                                type="button"
                                className="minus"
                                onClick={() => {
                                  countDown(
                                    setlfullwidthCounter,
                                    lfullwidthCounter
                                  );
                                }}
                              >
                                –
                              </button>
                              <Input
                                type="number"
                                className="product-quantity"
                                value={lfullwidthCounter}
                                min="0"
                                max="100"
                                readOnly
                              />
                              <button
                                type="button"
                                className="plus"
                                onClick={() => {
                                  countUP(
                                    setlfullwidthCounter,
                                    lfullwidthCounter
                                  );
                                }}
                              >
                                +
                              </button>
                            </div>
                          </div>
                        </Col>
                      </Row>
                    </div>

                    <Row className="mt-4 pt-2">
                      <h5 className="fs-13 fw-medium text-muted">Colored</h5>
                      <div className="d-flex flex-wrap align-items-start gap-2">
                        <div className="input-step step-primary">
                          <button
                            type="button"
                            className="minus"
                            onClick={() => {
                              countDown(setblueCounter, blueCounter);
                            }}
                          >
                            –
                          </button>
                          <Input
                            type="number"
                            className="product-quantity"
                            value={blueCounter}
                            min="0"
                            max="100"
                            readOnly
                          />
                          <button
                            type="button"
                            className="plus"
                            onClick={() => {
                              countUP(setblueCounter, blueCounter);
                            }}
                          >
                            +
                          </button>
                        </div>
                        <div className="input-step step-secondary">
                          <button
                            type="button"
                            className="minus"
                            onClick={() => {
                              countDown(setskyCounter, skyCounter);
                            }}
                          >
                            –
                          </button>
                          <Input
                            type="number"
                            className="product-quantity"
                            value={skyCounter}
                            min="0"
                            max="100"
                            readOnly
                          />
                          <button
                            type="button"
                            className="plus"
                            onClick={() => {
                              countUP(setskyCounter, skyCounter);
                            }}
                          >
                            +
                          </button>
                        </div>
                        <div className="input-step step-success">
                          <button
                            type="button"
                            className="minus"
                            onClick={() => {
                              countDown(setgreenCounter, greenCounter);
                            }}
                          >
                            –
                          </button>
                          <Input
                            type="number"
                            className="product-quantity"
                            value={greenCounter}
                            min="0"
                            max="100"
                            readOnly
                          />
                          <button
                            type="button"
                            className="plus"
                            onClick={() => {
                              countUP(setgreenCounter, greenCounter);
                            }}
                          >
                            +
                          </button>
                        </div>
                        <div className="input-step step-info">
                          <button
                            type="button"
                            className="minus"
                            onClick={() => {
                              countUP(settealCounter, tealCounter);
                            }}
                          >
                            –
                          </button>
                          <Input
                            type="number"
                            className="product-quantity"
                            value={tealCounter}
                            min="0"
                            max="100"
                            readOnly
                          />
                          <button
                            type="button"
                            className="plus"
                            onClick={() => {
                              countUP(settealCounter, tealCounter);
                            }}
                          >
                            +
                          </button>
                        </div>
                        <div className="input-step step-warning">
                          <button
                            type="button"
                            className="minus"
                            onClick={() => {
                              countDown(setyellowCounter, yellowCounter);
                            }}
                          >
                            –
                          </button>
                          <Input
                            type="number"
                            className="product-quantity"
                            value={yellowCounter}
                            min="0"
                            max="100"
                            readOnly
                          />
                          <button
                            type="button"
                            className="plus"
                            onClick={() => {
                              countUP(setyellowCounter, yellowCounter);
                            }}
                          >
                            +
                          </button>
                        </div>
                        <div className="input-step step-danger">
                          <button
                            type="button"
                            className="minus"
                            onClick={() => {
                              countDown(setredCounter, redCounter);
                            }}
                          >
                            –
                          </button>
                          <Input
                            type="number"
                            className="product-quantity"
                            value={redCounter}
                            min="0"
                            max="100"
                            readOnly
                          />
                          <button
                            type="button"
                            className="plus"
                            onClick={() => {
                              countUP(setredCounter, redCounter);
                            }}
                          >
                            +
                          </button>
                        </div>
                      </div>
                    </Row>
                  </div>
                </CardBody>
              </Card>
            </Col>
          </Row>

          <Row>
            <Col lg={12}>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Tranfer List</h4>
                </CardHeader>

                <CardBody>
                  <DualListbox />
                </CardBody>

              </Card>
            </Col>
          </Row>
        </Container>
      </div>
    </React.Fragment>
  );
};

export default FormAdvanced;