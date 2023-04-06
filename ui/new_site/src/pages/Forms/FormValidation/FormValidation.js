import React, { useState } from "react";
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import UiContent from "../../../Components/Common/UiContent";
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
import { BrowserDefaults, CustomStyles, SupportedElements } from "./FormValidationCode";

import {
  Row,
  Col,
  Card,
  CardBody,
  FormGroup,
  Button,
  Label,
  Input,
  Container,
  FormFeedback,
  Form,
  InputGroup,
} from "reactstrap";
// Formik validation
import * as Yup from "yup";
import { useFormik } from "formik";

const FormValidations = () => {
  document.title="Validation | Velzon - React Admin & Dashboard Template";

  // Form validation
  const validation = useFormik({
    // enableReinitialize : use this flag when initial values needs to be changed
    enableReinitialize: true,

    initialValues: {
      firstname: "",
      lastname: "",
      city: "",
      state: "",
      zip: "",
    },
    validationSchema: Yup.object({
      firstname: Yup.string().required("Please Enter Your First Name"),
      lastname: Yup.string().required("Please Enter Your Last Name"),
      city: Yup.string().required("Please Enter Your City"),
      state: Yup.string().required("Please Enter Your State"),
      zip: Yup.string().required("Please Enter Your Zip"),
    }),
    onSubmit: (values) => {
      console.log("values", values);
    },
  });

  const [formValidation, setValidation] = useState({
    fnm: null,
    lnm: null,
    unm: null,
    city: null,
    stateV: null,
  });

  function handleSubmit(e) {
    e.preventDefault();
    const modifiedV = { ...formValidation };
    var fnm = document.getElementById("validationTooltip01").value;
    var lnm = document.getElementById("validationTooltip02").value;
    var unm = document.getElementById("validationTooltipUsername").value;
    var city = document.getElementById("validationTooltip03").value;
    var stateV = document.getElementById("validationTooltip04").value;

    if (fnm === "") {
      modifiedV["fnm"] = false;
    } else {
      modifiedV["fnm"] = true;
    }

    if (lnm === "") {
      modifiedV["lnm"] = false;
    } else {
      modifiedV["lnm"] = true;
    }

    if (unm === "") {
      modifiedV["unm"] = false;
    } else {
      modifiedV["unm"] = true;
    }

    if (city === "") {
      modifiedV["city"] = false;
    } else {
      modifiedV["city"] = true;
    }

    if (stateV === "") {
      modifiedV["stateV"] = false;
    } else {
      modifiedV["stateV"] = true;
    }
    setValidation(modifiedV);
  }

  //for change tooltip display propery
  const onChangeValidation = (fieldName, value) => {
    const modifiedV = { ...validation };
    if (value !== "") {
      modifiedV[fieldName] = true;
    } else {
      modifiedV[fieldName] = false;
    }
    setValidation(modifiedV);
  };

  return (
    <React.Fragment>
      <UiContent />
      <div className="page-content">        
        <Container fluid={true}>
          <BreadCrumb title="Form Validation" pageTitle="Forms" />
          <Row>
            <Col lg={12}>
              <Card>
                <PreviewCardHeader title="Browser defaults" />
                <CardBody>
                  <p className="text-muted">Not interested in custom validation feedback messages or writing JavaScript to change form behaviors? All good, you can use the browser defaults. Try submitting the form below. Depending on your browser and OS, you’ll see a slightly different style of feedback.While these feedback styles cannot be styled with CSS, you can still customize the feedback text through JavaScript.</p>

                  <div className="live-preview">
                    <form className="row g-3">
                      <Col md={4}>
                        <Label for="validationDefault01" className="form-label">First name</Label>
                        <Input type="text" className="form-control" id="validationDefault01" defaultValue="Mark" required />
                      </Col>
                      <Col md={4}>
                        <Label for="validationDefault02" className="form-label">Last name</Label>
                        <Input type="text" className="form-control" id="validationDefault02" defaultValue="Otto" required />
                      </Col>
                      <Col md={4}>
                        <Label for="validationDefaultUsername" className="form-label">Username</Label>
                        <InputGroup>
                          <span className="input-group-text" id="inputGroupPrepend2">@</span>
                          <Input type="text" className="form-control" id="validationDefaultUsername" aria-describedby="inputGroupPrepend2"
                            required />
                        </InputGroup>
                      </Col>
                      <Col md={6}>
                        <Label for="validationDefault03" className="form-label">City</Label>
                        <Input type="text" className="form-control" id="validationDefault03" required />
                      </Col>
                      <Col md={3}>
                        <Label for="validationDefault04" className="form-label">State</Label>
                        <select className="form-select" id="validationDefault04" required>
                          <option disabled defaultValue="">Choose...</option>
                          <option>...</option>
                        </select>
                      </Col>
                      <Col md={3}>
                        <Label for="validationDefault05" className="form-label">Zip</Label>
                        <Input type="text" className="form-control" id="validationDefault05" required />
                      </Col>
                      <Col xs={12}>
                        <div className="form-check">
                          <Input className="form-check-input" type="checkbox" defaultValue="" id="invalidCheck2" required />
                          <Label className="form-check-label" for="invalidCheck2">
                            Agree to terms and conditions
                          </Label>
                        </div>
                      </Col>
                      <Col xs={12}>
                        <button className="btn btn-primary" type="submit">Submit form</button>
                      </Col>
                    </form>
                  </div>
                  <div className="d-none code-view">
                    <pre className="language-markup" style={{ "height": "352px" }}>
                      <code>
                        <BrowserDefaults />
                      </code>
                    </pre>
                  </div>
                </CardBody>
              </Card>
            </Col>
          </Row>

          <Row>
            <Col lg={12}>
              <Card>
                <PreviewCardHeader title="Custom Styles" />
                <CardBody>
                  <p className="text-muted">
                    For custom Bootstrap form validation messages, you’ll need to add the <code>novalidate</code> boolean attribute to your <code>&lt;form&gt;</code>. This disables the browser default feedback tooltips, but still provides access to the form validation APIs in JavaScript. Try to submit the form below; our JavaScript will intercept the submit button and relay feedback to you. When attempting to submit, you’ll see the <code>:invalid</code> and <code>:valid</code> styles applied to your form controls.
                  </p>
                  <div className="live-preview">
                    <Form
                      className="needs-validation"
                      onSubmit={(e) => {
                        e.preventDefault();
                        validation.handleSubmit();
                        return false;
                      }}
                    >
                      <Row>
                        <Col md="6">
                          <FormGroup className="mb-3">
                            <Label htmlFor="validationCustom01">First name</Label>
                            <Input
                              name="firstname"
                              placeholder="Mark"
                              type="text"
                              className="form-control"
                              id="validationCustom01"
                              onChange={validation.handleChange}
                              onBlur={validation.handleBlur}
                              value={validation.values.firstname || ""}
                              invalid={
                                validation.touched.firstname &&
                                  validation.errors.firstname
                                  ? true
                                  : false
                              }
                            />
                            {validation.touched.firstname &&
                              validation.errors.firstname ? (
                              <FormFeedback type="invalid">
                                {validation.errors.firstname}
                              </FormFeedback>
                            ) : null}
                          </FormGroup>
                        </Col>
                        <Col md="6">
                          <FormGroup className="mb-3">
                            <Label htmlFor="validationCustom02">Last name</Label>
                            <Input
                              name="lastname"
                              placeholder="Otto"
                              type="text"
                              className="form-control"
                              id="validationCustom02"
                              onChange={validation.handleChange}
                              onBlur={validation.handleBlur}
                              value={validation.values.lastname || ""}
                              invalid={
                                validation.touched.lastname &&
                                  validation.errors.lastname
                                  ? true
                                  : false
                              }
                            />
                            {validation.touched.lastname &&
                              validation.errors.lastname ? (
                              <FormFeedback type="invalid">
                                {validation.errors.lastname}
                              </FormFeedback>
                            ) : null}
                          </FormGroup>
                        </Col>
                      </Row>
                      <Row>
                        <Col md="4">
                          <FormGroup className="mb-3">
                            <Label htmlFor="validationCustom03">City</Label>
                            <Input
                              name="city"
                              placeholder="City"
                              type="text"
                              className="form-control"
                              onChange={validation.handleChange}
                              onBlur={validation.handleBlur}
                              value={validation.values.city || ""}
                              invalid={
                                validation.touched.city && validation.errors.city
                                  ? true
                                  : false
                              }
                            />
                            {validation.touched.city && validation.errors.city ? (
                              <FormFeedback type="invalid">
                                {validation.errors.city}
                              </FormFeedback>
                            ) : null}
                          </FormGroup>
                        </Col>
                        <Col md="4">
                          <FormGroup className="mb-3">
                            <Label htmlFor="validationCustom04">State</Label>
                            <Input
                              name="state"
                              placeholder="State"
                              type="text"
                              className="form-control"
                              id="validationCustom04"
                              onChange={validation.handleChange}
                              onBlur={validation.handleBlur}
                              value={validation.values.state || ""}
                              invalid={
                                validation.touched.state &&
                                  validation.errors.state
                                  ? true
                                  : false
                              }
                            />
                            {validation.touched.state &&
                              validation.errors.state ? (
                              <FormFeedback type="invalid">
                                {validation.errors.state}
                              </FormFeedback>
                            ) : null}
                          </FormGroup>
                        </Col>
                        <Col md="4">
                          <FormGroup className="mb-3">
                            <Label htmlFor="validationCustom05">Zip</Label>
                            <Input
                              name="zip"
                              placeholder="Zip"
                              type="text"
                              className="form-control"
                              id="validationCustom05"
                              onChange={validation.handleChange}
                              onBlur={validation.handleBlur}
                              value={validation.values.zip || ""}
                              invalid={
                                validation.touched.zip && validation.errors.zip
                                  ? true
                                  : false
                              }
                            />
                            {validation.touched.zip && validation.errors.zip ? (
                              <FormFeedback type="invalid">
                                {validation.errors.zip}
                              </FormFeedback>
                            ) : null}
                          </FormGroup>
                        </Col>
                      </Row>
                      <Row>
                        <Col lg="12">
                          <FormGroup className="mb-3">
                            <div className="form-check">
                              <Input
                                type="checkbox"
                                className="form-check-input"
                                id="invalidCheck"
                              />
                              <Label
                                className="form-check-label"
                                htmlFor="invalidCheck"
                              >
                                {" "}
                                Agree to terms and conditions
                              </Label>
                            </div>
                          </FormGroup>
                        </Col>
                      </Row>
                      <Button color="primary" type="submit">
                        Submit form
                      </Button>
                    </Form>
                  </div>
                  <div className="d-none code-view">
                    <pre className="language-markup" style={{ "height": "352px" }}>
                      <code>
                        <CustomStyles />
                      </code>
                    </pre>
                  </div>
                </CardBody>
              </Card>
            </Col>
          </Row>
          <Row>
            <Col lg={12}>
              <Card>
                <PreviewCardHeader title="Supported Elements" />

                <CardBody>
                  <p className="text-muted">Block-level or inline-level form text can be created using <code>.form-text</code>.</p>

                  <div className="live-preview">
                    <Form className="was-validated">
                      <div className="mb-3">
                        <Label htmlFor="validationTextarea" className="form-label">Textarea</Label>
                        <textarea className="form-control" id="validationTextarea" placeholder="Required example textarea"
                          required></textarea>
                        <div className="invalid-feedback">
                          Please enter a message in the textarea.
                        </div>
                      </div>

                      <div className="form-check mb-3">
                        <Input type="checkbox" className="form-check-input" id="validationFormCheck1" required />
                        <Label className="form-check-label" htmlFor="validationFormCheck1">Check this checkbox</Label>
                        <div className="invalid-feedback">Example invalid feedback text</div>
                      </div>

                      <div className="form-check">
                        <Input type="radio" className="form-check-input" id="validationFormCheck2" name="radio-stacked" required />
                        <Label className="form-check-label" htmlFor="validationFormCheck2">Toggle this radio</Label>
                      </div>
                      <div className="form-check mb-3">
                        <Input type="radio" className="form-check-input" id="validationFormCheck3" name="radio-stacked" required />
                        <Label className="form-check-label" for="validationFormCheck3">Or toggle this other radio</Label>
                        <div className="invalid-feedback">More example invalid feedback text</div>
                      </div>

                      <div className="mb-3">
                        <select className="form-select" required aria-label="select example">
                          <option value="">Open this select menu</option>
                          <option value="1">One</option>
                          <option value="2">Two</option>
                          <option value="3">Three</option>
                        </select>
                        <div className="invalid-feedback">Example invalid select feedback</div>
                      </div>

                      <div className="mb-3">
                        <Input type="file" className="form-control" aria-label="file example" required />
                        <div className="invalid-feedback">Example invalid form file feedback</div>
                      </div>

                      <div className="mb-0">
                        <button className="btn btn-primary" type="submit" disabled>Submit form</button>
                      </div>
                    </Form>
                  </div>
                  <div className="d-none code-view">
                    <pre className="language-markup" style={{ "height": "352px" }}>
                      <code>
                        <SupportedElements />
                      </code>
                    </pre>
                  </div>

                </CardBody>
              </Card>
            </Col>
          </Row>
          <Row>
            <Col>
              <Card>
                <PreviewCardHeader title="Tooltips" />
                <CardBody>
                  <p className="text-muted">
                    If your form layout allows it, you can swap the
                    <code>.{"{valid|invalid-}"}feedback</code> classes for
                    <code>.{"{valid|invalid-}"}-tooltip</code> classes to
                    display validation feedback in a styled tooltip.
                  </p>
                  <div className="live-preview">
                    <form
                      className="needs-validation"
                      method="post"
                      id="tooltipForm"
                      onSubmit={(e) => {
                        handleSubmit(e);
                      }}
                    >
                      <Row>
                        <Col md="4">
                          <div className="mb-3 position-relative">
                            <Label htmlFor="validationTooltip01">
                              First name
                            </Label>
                            <Input
                              type="text"
                              className="form-control"
                              id="validationTooltip01"
                              placeholder="Mark"
                              defaultValue="Mark"
                              onChange={(event) => {
                                onChangeValidation("fnm", event.target.value);
                              }}
                              valid={validation["fnm"] === true}
                              invalid={
                                validation["fnm"] !== true &&
                                validation["fnm"] !== null
                              }
                            />

                            <div
                              className={
                                validation["fnm"] === true
                                  ? "valid-tooltip"
                                  : "invalid-tooltip"
                              }
                              name="validate"
                              id="validate1"
                            >
                              {validation["fnm"] === true
                                ? "Looks good!"
                                : "Please Enter Valid First Name"}
                            </div>
                          </div>
                        </Col>
                        <Col md="4">
                          <div className="mb-3 position-relative">
                            <Label htmlFor="validationTooltip02">Last name</Label>
                            <Input
                              type="text"
                              className="form-control"
                              id="validationTooltip02"
                              placeholder="Otto"
                              onChange={(event) =>
                                onChangeValidation("lnm", event.target.value)
                              }
                              valid={validation["lnm"] === true}
                              invalid={
                                validation["lnm"] !== true &&
                                validation["lnm"] !== null
                              }
                            />
                            <div
                              className={
                                validation["lnm"] === true
                                  ? "valid-tooltip"
                                  : "invalid-tooltip"
                              }
                              name="validate"
                              id="validate2"
                            >
                              {validation["lnm"] === true
                                ? "Looks good!"
                                : "Please Enter Valid Last Name"}
                            </div>
                          </div>
                        </Col>
                        <Col md="4">
                          <div className="mb-3 position-relative">
                            <Label htmlFor="validationTooltipUsername">
                              Username
                            </Label>
                            <div className="input-group">
                              <div className="input-group-prepend">
                                <span
                                  className="input-group-text"
                                  id="validationTooltipUsernamePrepend"
                                >
                                  @
                                </span>
                              </div>
                              <Input
                                type="text"
                                className="form-control"
                                id="validationTooltipUsername"
                                placeholder="Username"
                                onChange={(event) =>
                                  onChangeValidation("unm", event.target.value)
                                }
                                valid={validation["unm"] === true}
                                invalid={
                                  validation["unm"] !== true &&
                                  validation["unm"] !== null
                                }
                              />
                              <div
                                className={
                                  validation["unm"] === true
                                    ? "valid-tooltip"
                                    : "invalid-tooltip"
                                }
                                name="validate"
                                id="validate3"
                              >
                                {validation["unm"] === true
                                  ? "Looks good!"
                                  : "Please choose a unique and valid username."}
                              </div>
                            </div>
                          </div>
                        </Col>
                      </Row>
                      <Row>
                        <Col md="6">
                          <div className="mb-3 position-relative">
                            <Label htmlFor="validationTooltip03">City</Label>
                            <Input
                              type="text"
                              className="form-control"
                              id="validationTooltip03"
                              placeholder="City"
                              onChange={(event) =>
                                onChangeValidation("city", event.target.value)
                              }
                              valid={validation["city"] === true}
                              invalid={
                                validation["city"] !== true &&
                                validation["city"] !== null
                              }
                            />
                            <div
                              className={
                                validation["city"] === true
                                  ? "valid-tooltip"
                                  : "invalid-tooltip"
                              }
                              name="validate"
                              id="validate4"
                            >
                              {validation["city"] === true
                                ? "Looks good!"
                                : "Please choose a unique and valid username.Please provide a valid city."}
                            </div>
                          </div>
                        </Col>
                        <Col md="6">
                          <div className="mb-3 position-relative">
                            <Label htmlFor="validationTooltip04">State</Label>
                            <Input
                              type="text"
                              className="form-control"
                              id="validationTooltip04"
                              placeholder="State"
                              onChange={(event) =>
                                onChangeValidation("stateV", event.target.value)
                              }
                              valid={validation["stateV"] === true}
                              invalid={
                                validation["stateV"] !== true &&
                                validation["stateV"] !== null
                              }
                            />
                            <div
                              className={
                                validation["stateV"] === true
                                  ? "valid-tooltip"
                                  : "invalid-tooltip"
                              }
                              name="validate"
                              id="validate5"
                            >
                              {validation["stateV"] === true
                                ? "Looks good!"
                                : "Please provide a valid state."}
                            </div>
                          </div>
                        </Col>
                      </Row>
                      <Button color="primary" type="submit">
                        Submit form
                      </Button>
                    </form>
                  </div>
                  <div className="d-none code-view">
                    <pre className="language-markup" style={{ "height": "352px" }}>
                      <code>
                        <BrowserDefaults />
                      </code>
                    </pre>
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

export default FormValidations;
