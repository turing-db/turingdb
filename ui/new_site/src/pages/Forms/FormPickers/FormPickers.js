import React, { useState } from 'react';
import { Card, CardBody, CardHeader, Container, Form, Row, Col, Label } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import ColorPicker from "@vtaits/react-color-picker";
import { SketchPicker } from "react-color";

//Import Flatepicker
import Flatpickr from "react-flatpickr";

const FormPickers = () => {
  document.title="Pickers | Velzon - React Admin & Dashboard Template";

  const [color, setcolor] = useState("rgba(3, 142, 220, 1)");
  const [colorCust, setcolorCust] = useState("rgba(95, 208, 243, 1)");
  const [colorRGBA, setcolorRGBA] = useState("rgba(247, 204, 83, 1)");
  
  const [display_RGBA, setdisplay_RGBA] = useState(false);
  const [display_Cust, setdisplay_Cust] = useState(false);
  
  function handleRGBA() {
    setdisplay_RGBA(!display_RGBA);
  }
  function handleCust() {
    setdisplay_Cust(!display_Cust);
  }
  const onSwatchHover_RGBA = (color) => {
    const format =
      "rgba(" +
      color.rgb.r +
      "," +
      color.rgb.g +
      "," +
      color.rgb.b +
      "," +
      color.rgb.a +
      ")";
    setcolorRGBA(format);
  };

  const onSwatchHover_Cust = (color) => {
    const format1 =
      "rgba(" +
      color.rgb.r +
      "," +
      color.rgb.g +
      "," +
      color.rgb.b +
      "," +
      color.rgb.a +
      ")";
      setcolorCust(format1);
  };

  const [simple_color, setsimple_color] = useState(0);

  const onDrag = (c1) => {
    setcolor(c1);
  };


  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="Pickers" pageTitle="Forms" />

          <Row>
            <Col lg={12}>
              <Card >
                <CardHeader >
                  <h4 className="card-title mb-0">Flatpickr - Datepicker</h4>
                </CardHeader>
                <CardBody>
                  <Form >
                    <Row className="gy-3">
                      <Col lg={6}>
                        <div>
                          <Label className="form-label mb-0">Basic</Label>
                        <Flatpickr
                          className="form-control"
                          options={{
                            dateFormat: "d M, Y"
                          }}
                        />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div>
                          <Label className="form-label mb-0">DateTime</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              enableTime: true,
                              dateFormat: "Y-m-d H:i",
                            }}
                          />
                        </div>
                      </Col>
                    </Row>
                    <Row >
                      <Col lg={6}>
                        <div className="mt-3">
                          <Label className="form-label mb-0">Human-Friendly Dates</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              altInput: true,
                              altFormat: "F j, Y",
                              dateFormat: "Y-m-d",
                            }}
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mt-3">
                          <Label className="form-label mb-0">MinDate and MaxDate</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              minDate: "today",
                              maxDate: new Date().fp_incr(14)
                            }}
                          />
                        </div>
                      </Col>
                    </Row>
                    <Row>
                      <Col lg={6} >
                        <div className="mt-3">
                          <Label className="form-label mb-0">Default Date</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              dateFormat: "Y-m-d",
                              defaultDate: ["2022-01-20"]
                            }}
                          />
                        </div>
                      </Col>
                      <Col lg={6} >
                        <div className="mt-3">
                          <Label className="form-label mb-0">Disabling Dates</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              disable: ["2022-01-30", "2022-02-21", "2022-03-08", new Date(2025, 4, 9)],
                              dateFormat: "Y-m-d",
                            }}
                          />
                        </div>
                      </Col>
                    </Row>
                    <Row>
                      <Col lg={6}>
                        <div className="mt-3">
                          <Label className="form-label mb-0">Selecting Multiple Dates</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              mode: "multiple",
                              dateFormat: "Y-m-d"
                            }}
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mt-3">
                          <Label className="form-label mb-0">Range</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              mode: "range",
                              dateFormat: "Y-m-d"
                            }}
                          />
                        </div>
                      </Col>
                    </Row>
                    <Row>
                      <Col lg={6} >
                        <div className="mt-3">
                          <Label className="form-label mb-0">Inline</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              inline: true,
                              dateFormat: "Y-m-d"
                            }}
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mt-3">
                          <label className="form-label mb-0">Week Numbers</label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              weekNumbers: true,
                              altInput: true,
                              altFormat: "F j, Y",
                              dateFormat: "Y-m-d",
                            }}
                          />
                        </div>
                      </Col>
                    </Row>
                  </Form>
                </CardBody>
              </Card>
            </Col>
          </Row>
          <Row>
            <Col lg={12}>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Flatpickr - Timepicker</h4>
                </CardHeader>
                <CardBody>
                  <Form >
                    <Row className="gy-3">
                      <Col lg={6}>
                        <div>
                          <Label className="form-label mb-0">Timepicker</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              enableTime: true,
                              noCalendar: true,
                              dateFormat: "H:i",
                            }}
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div>
                          <Label className="form-label mb-0">24-hour Time Picker</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              enableTime: true,
                              noCalendar: true,
                              dateFormat: "H:i",
                              time_24hr: true
                            }}
                          />
                        </div>
                      </Col>
                    </Row>
                    <Row>
                      <Col lg={6}>
                        <div className="mt-3">
                          <Label className="form-label mb-0">Time Picker w/ Limits</Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              enableTime: true,
                              noCalendar: true,
                              dateFormat: "H:i",
                              minTime: "16:00",
                              maxTime: "22:30",
                            }}
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mt-3">
                          <label className="form-label mb-0">Preloading Time</label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              enableTime: true,
                              noCalendar: true,
                              dateFormat: "H:i",
                              defaultDate: "13:45"
                            }}
                          />
                        </div>
                      </Col>
                    </Row>
                    <Row>
                      <Col lg={6}>
                        <div className="mt-3">
                          <label className="form-label mb-0">Inline</label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              enableTime: true,
                              noCalendar: true,
                              inline: true,
                            }}
                          />
                        </div>
                      </Col>
                    </Row>
                  </Form>
                </CardBody>
              </Card>
            </Col>
          </Row>

          <Row>
            <Col lg={12}>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Colorpicker</h4>
                </CardHeader>

                <CardBody>
                  <div>
                    <div>
                      <h5 className="fs-14 mb-3">Themes</h5>
                      <Row className="g-4">
                        <Col lg={4} md={6}>
                          <div>
                            <h5 className="fs-13 text-muted mb-2">Classic Demo</h5>
                            
                            <div
                              className="classic-colorpicker"
                              onClick={() => {
                                setsimple_color(!simple_color);
                              }}
                            >
                              <i
                                style={{
                                  height: "28px",
                                  width: "28px",
                                  background: color,
                                  display: "block"
                                }}
                              />
                            
                          </div>
                          {simple_color ? (
                            <ColorPicker
                              saturationHeight={100}
                              saturationWidth={100}
                              value={color}
                              onDrag={onDrag}
                            />
                          ) : null}
                          </div>
                        </Col>
                        <Col lg={4} md={6}>
                          <div>
                            <h5 className="fs-13 text-muted mb-2">RGBA Demo</h5>                         
                            <div
                              className="monolith-colorpicker"
                              onClick={handleCust}
                            >
                              <i
                                style={{
                                  height: "28px",
                                  width: "28px",
                                  background: colorCust,
                                  display: "block"
                                }}
                              />
                            </div>
                         
                          {display_Cust ? (
                            <SketchPicker
                            color="#fff"
                            value={colorCust}
                            width="160px"
                            onChangeComplete={onSwatchHover_Cust}
                          />
                        ) : null}
                          </div>
                          
                        </Col>
                        <Col lg={4} md={6}>
                          <div>
                            <h5 className="fs-13 text-muted mb-2">Nano Demo</h5>
                            
                            <div
                              className="nano-colorpicker"
                              onClick={handleRGBA}
                            >
                              <i
                                style={{
                                  height: "28px",
                                  width: "28px",
                                  background: colorRGBA,
                                  display: "block"
                                }}
                              />
                            </div>
                          
                          {display_RGBA ? (
                            <SketchPicker
                              color="#fff"
                              value={colorRGBA}
                              width="160px"
                              onChangeComplete={onSwatchHover_RGBA}
                            />
                          ) : null}
                          </div>
                        </Col>
                      </Row>
                    </div>                   
                  </div>
                </CardBody>
              </Card>
            </Col>
          </Row>

        </Container>
      </div>
    </React.Fragment>
  );
};

export default FormPickers;