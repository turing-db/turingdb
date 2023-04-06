import React, { useState } from "react";
import {
  Card,
  CardBody,
  CardHeader,
  Col,
  Container,
  Form,
  Input,
  Label,
  Nav,
  NavItem,
  NavLink,
  Row,
  TabContent,
  TabPane,
  Progress,
} from "reactstrap";
//Import images
import dummyUser from "../../../assets/images/users/user-dummy-img.jpg";
import logoDark from "../../../assets/images/logo-dark.png";

import classnames from "classnames";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import { loadAnimation } from "lottie-web";
import { defineElement } from "lord-icon-element";
// register lottie and define custom element
defineElement(loadAnimation);

const FormWizard = () => {
  document.title = "Wizard | Velzon - React Admin & Dashboard Template";

  const [activeTab, setactiveTab] = useState(1);
  const [activeArrowTab, setactiveArrowTab] = useState(4);
  const [activeVerticalTab, setactiveVerticalTab] = useState(7);
  const [progressbarvalue, setprogressbarvalue] = useState(0);
  const [passedSteps, setPassedSteps] = useState([1]);
  const [passedarrowSteps, setPassedarrowSteps] = useState([1]);
  const [passedverticalSteps, setPassedverticalSteps] = useState([1]);

  function toggleTab(tab, value) {
    if (activeTab !== tab) {
      var modifiedSteps = [...passedSteps, tab];

      if (tab >= 1 && tab <= 4) {
        setactiveTab(tab);
        setPassedSteps(modifiedSteps);
      }
    }
    setprogressbarvalue(value);
  }

  function toggleArrowTab(tab) {
    if (activeArrowTab !== tab) {
      var modifiedSteps = [...passedarrowSteps, tab];

      if (tab >= 4 && tab <= 7) {
        setactiveArrowTab(tab);
        setPassedarrowSteps(modifiedSteps);
      }
    }
  }

  function toggleVerticalTab(tab) {
    if (activeVerticalTab !== tab) {
      var modifiedSteps = [...passedverticalSteps, tab];

      if (tab >= 7 && tab <= 11) {
        setactiveVerticalTab(tab);
        setPassedverticalSteps(modifiedSteps);
      }
    }
  }

  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="Wizard" pageTitle="Forms" />
          <Row>
            <Col xl={6}>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Progress Nav Steps</h4>
                </CardHeader>
                <CardBody >
                  <Form className="form-steps" action="#" autoComplete="off">
                    <div className="text-center pt-3 pb-4 mb-1">
                      <h5>Signup Your Account</h5>
                    </div>

                    <div className="progress-nav mb-4">
                      <Progress
                        value={progressbarvalue}
                        style={{ height: "1px" }}
                      />

                      <Nav
                        className="nav-pills progress-bar-tab custom-nav"
                        role="tablist"

                      >
                        <NavItem>
                          <NavLink
                            to="#"
                            id="pills-gen-info-tab"
                            className={classnames(
                              {
                                active: activeTab === 1,
                                done: activeTab <= 4 && activeTab >= 0,
                              },
                              "rounded-pill"
                            )}
                            onClick={() => {
                              toggleTab(1, 0);
                            }}
                            tag="button"
                          >
                            1
                          </NavLink>
                        </NavItem>
                        <NavItem>
                          <NavLink
                            to="#"
                            id="pills-gen-info-tab"
                            className={classnames(
                              {
                                active: activeTab === 2,
                                done: activeTab <= 4 && activeTab > 1,
                              },
                              "rounded-pill"
                            )}
                            onClick={() => {
                              toggleTab(2, 50);
                            }}
                            tag="button"
                          >
                            2
                          </NavLink>
                        </NavItem>
                        <NavItem>
                          <NavLink
                            to="#"
                            id="pills-gen-info-tab"
                            className={classnames(
                              {
                                active: activeTab === 3,
                                done: activeTab <= 4 && activeTab > 2,
                              },
                              "rounded-pill"
                            )}
                            onClick={() => {
                              toggleTab(3, 100);
                            }}
                            tag="button"
                          >
                            3
                          </NavLink>
                        </NavItem>
                      </Nav>
                    </div>

                    <TabContent activeTab={activeTab}>
                      <TabPane tabId={1}>
                        <div>
                          <div className="mb-4">
                            <div>
                              <h5 className="mb-1">General Information</h5>
                              <p className="text-muted">
                                Fill all Information as below
                              </p>
                            </div>
                          </div>
                          <Row>
                            <Col lg={6}>
                              <div className="mb-3">
                                <Label
                                  className="form-label"
                                  htmlFor="gen-info-email-input"
                                >
                                  Email
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="gen-info-email-input"
                                  placeholder="Enter email"
                                />
                                <div className="invalid-feedback">Please enter an email address</div>
                              </div>
                            </Col>
                            <Col lg={6}>
                              <div className="mb-3">
                                <Label
                                  className="form-label"
                                  htmlFor="gen-info-username-input"
                                >
                                  User Name
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="gen-info-username-input"
                                  placeholder="Enter user name"
                                />
                                <div className="invalid-feedback">Please enter a user name</div>
                              </div>
                            </Col>
                          </Row>
                          <div className="mb-3">
                            <Label
                              className="form-label"
                              htmlFor="gen-info-password-input"
                            >
                              Password
                            </Label>
                            <Input
                              type="password"
                              className="form-control"
                              id="gen-info-password-input"
                              placeholder="Enter Password"
                            />
                            <div className="invalid-feedback">Please enter a password</div>
                          </div>
                        </div>
                        <div className="d-flex align-items-start gap-3 mt-4">
                          <button
                            type="button"
                            className="btn btn-success btn-label right ms-auto nexttab nexttab"
                            data-nexttab="pills-info-desc-tab"
                            onClick={() => {
                              toggleTab(activeTab + 1, 50);
                            }}
                          >
                            <i className="ri-arrow-right-line label-icon align-middle fs-16 ms-2"></i>
                            Go to more info
                          </button>
                        </div>
                      </TabPane>

                      <TabPane tabId={2}>
                        <div>
                          <div className="text-center">
                            <div className="profile-user position-relative d-inline-block mx-auto mb-2">
                              <img
                                src={dummyUser}
                                className="rounded-circle avatar-lg img-thumbnail user-profile-image"
                                alt=""
                              />
                              <div className="avatar-xs p-0 rounded-circle profile-photo-edit">
                                <Input
                                  id="profile-img-file-input"
                                  type="file"
                                  className="profile-img-file-input"
                                  accept="image/png, image/jpeg"
                                />
                                <Label
                                  htmlFor="profile-img-file-input"
                                  className="profile-photo-edit avatar-xs"
                                >
                                  <span className="avatar-title rounded-circle bg-light text-body">
                                    <i className="ri-camera-fill"></i>
                                  </span>
                                </Label>
                              </div>
                            </div>
                            <h5 className="fs-14">Add Image</h5>
                          </div>
                          <div>
                            <Label
                              className="form-label"
                              htmlFor="gen-info-description-input"
                            >
                              Description
                            </Label>
                            <Input
                              type="textarea"
                              className="form-control"
                              placeholder="Enter Description"
                              id="gen-info-description-input"
                              rows="2"
                            ></Input>
                          </div>
                        </div>
                        <div className="d-flex align-items-start gap-3 mt-4">
                          <button
                            type="button"
                            className="btn btn-link text-decoration-none btn-label previestab"
                            onClick={() => {
                              toggleTab(activeTab - 1, 0);
                            }}
                          >
                            <i className="ri-arrow-left-line label-icon align-middle fs-16 me-2"></i>{" "}
                            Back to General
                          </button>
                          <button
                            type="button"
                            className="btn btn-success btn-label right ms-auto nexttab nexttab"
                            onClick={() => {
                              toggleTab(activeTab + 1, 100);
                            }}
                          >
                            <i className="ri-arrow-right-line label-icon align-middle fs-16 ms-2"></i>
                            Submit
                          </button>
                        </div>
                      </TabPane>

                      <TabPane tabId={3}>
                        <div>
                          <div className="text-center">
                            <div className="mb-4">
                              <lord-icon
                                src="https://cdn.lordicon.com/lupuorrc.json"
                                trigger="loop"
                                colors="primary:#02a8b5,secondary:#405189"
                                style={{ width: "120px", height: "120px" }}
                              ></lord-icon>
                            </div>
                            <h5>Well Done !</h5>
                            <p className="text-muted">
                              You have Successfully Signed Up
                            </p>
                          </div>
                        </div>
                      </TabPane>
                    </TabContent>
                  </Form>
                </CardBody>
              </Card>
            </Col>

            <Col xl={6}>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Arrow Nav Steps</h4>
                </CardHeader>
                <CardBody >
                  <Form className="form-steps" autoComplete="off">
                    <div className="text-center pt-3 pb-4 mb-1">
                      <img src={logoDark} alt="" height="17" />
                    </div>
                    <div className="step-arrow-nav mb-4">
                      <Nav
                        className="nav-pills custom-nav nav-justified"
                        role="tablist"
                      >
                        <NavItem>
                          <NavLink
                            href="#"
                            id="steparrow-gen-info-tab"
                            className={classnames({
                              active: activeArrowTab === 4,
                              done: activeArrowTab <= 6 && activeArrowTab > 3,
                            })}
                            onClick={() => {
                              toggleArrowTab(4);
                            }}
                          >
                            General
                          </NavLink>
                        </NavItem>
                        <NavItem>
                          <NavLink
                            href="#"
                            id="steparrow-gen-info-tab"
                            className={classnames({
                              active: activeArrowTab === 5,
                              done: activeArrowTab <= 6 && activeArrowTab > 4,
                            })}
                            onClick={() => {
                              toggleArrowTab(5);
                            }}
                          >
                            Description
                          </NavLink>
                        </NavItem>
                        <NavItem>
                          <NavLink
                            href="#"
                            id="steparrow-gen-info-tab"
                            className={classnames({
                              active: activeArrowTab === 6,
                              done: activeArrowTab <= 6 && activeArrowTab > 5,
                            })}
                            onClick={() => {
                              toggleArrowTab(6);
                            }}
                          >
                            Finish
                          </NavLink>
                        </NavItem>
                      </Nav>
                    </div>

                    <TabContent activeTab={activeArrowTab}>
                      <TabPane id="steparrow-gen-info" tabId={4}>
                        <div>
                          <Row>
                            <Col lg={6}>
                              <div className="mb-3">
                                <Label
                                  className="form-label"
                                  htmlFor="steparrow-gen-info-email-input"
                                >
                                  Email
                                </Label>
                                <Input
                                  type="email"
                                  className="form-control"
                                  id="steparrow-gen-info-email-input"
                                  placeholder="Enter email"
                                  required
                                />
                                <div className="invalid-feedback">Please enter an email address</div>
                              </div>
                            </Col>
                            <Col lg={6}>
                              <div className="mb-3">
                                <Label
                                  className="form-label"
                                  htmlFor="steparrow-gen-info-username-input"
                                >
                                  User Name
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="steparrow-gen-info-username-input"
                                  placeholder="Enter user name"
                                  required
                                />
                                <div className="invalid-feedback">Please enter a user name</div>
                              </div>
                            </Col>
                          </Row>
                          <div className="mb-3">
                            <Label
                              className="form-label"
                              htmlFor="steparrow-gen-info-password-input"
                            >
                              Password
                            </Label>
                            <Input
                              type="password"
                              className="form-control"
                              id="steparrow-gen-info-password-input"
                              placeholder="Enter password"
                              required
                            />
                            <div className="invalid-feedback">Please enter a password</div>
                          </div>
                          <div>
                            <Label
                              className="form-label"
                              htmlFor="steparrow-gen-info-confirm-password-input"
                            >
                              Confirm Password
                            </Label>
                            <Input
                              type="password"
                              className="form-control"
                              id="steparrow-gen-info-confirm-password-input"
                              placeholder="Enter confirm password"
                            />
                            <div className="invalid-feedback">Please enter a confirm password</div>
                          </div>
                        </div>
                        <div className="d-flex align-items-start gap-3 mt-4">
                          <button
                            type="button"
                            className="btn btn-success btn-label right ms-auto nexttab nexttab"
                            data-nexttab="steparrow-description-info-tab"
                            onClick={() => {
                              toggleArrowTab(activeArrowTab + 1);
                            }}
                          >
                            <i className="ri-arrow-right-line label-icon align-middle fs-16 ms-2"></i>
                            Go to more info
                          </button>
                        </div>
                      </TabPane>

                      <TabPane id="steparrow-description-info" tabId={5}>
                        <div>
                          <div className="mb-3">
                            <Label htmlFor="formFile" className="form-label">
                              Upload Image
                            </Label>
                            <Input
                              className="form-control"
                              type="file"
                              id="formFile"
                            />
                          </div>
                          <div>
                            <Label
                              className="form-label"
                              htmlFor="des-info-description-input"
                            >
                              Description
                            </Label>
                            <textarea
                              className="form-control"
                              placeholder="Enter Description"
                              id="des-info-description-input"
                              rows="3"
                              required
                            ></textarea>
                            <div className="invalid-feedback">Please enter a description</div>
                          </div>
                        </div>
                        <div className="d-flex align-items-start gap-3 mt-4">
                          <button
                            type="button"
                            className="btn btn-light btn-label previestab"
                            onClick={() => {
                              toggleArrowTab(activeArrowTab - 1);
                            }}
                          >
                            <i className="ri-arrow-left-line label-icon align-middle fs-16 me-2"></i>{" "}
                            Back to General
                          </button>
                          <button
                            type="button"
                            className="btn btn-success btn-label right ms-auto nexttab nexttab"
                            onClick={() => {
                              toggleArrowTab(activeArrowTab + 1);
                            }}
                            data-nexttab="pills-experience-tab"
                          >
                            <i className="ri-arrow-right-line label-icon align-middle fs-16 ms-2"></i>
                            Submit
                          </button>
                        </div>
                      </TabPane>

                      <TabPane id="pills-experience" tabId={6}>
                        <div className="text-center">
                          <div className="avatar-md mt-5 mb-4 mx-auto">
                            <div className="avatar-title bg-light text-success display-4 rounded-circle">
                              <i className="ri-checkbox-circle-fill"></i>
                            </div>
                          </div>
                          <h5>Well Done !</h5>
                          <p className="text-muted">
                            You have Successfully Signed Up
                          </p>
                        </div>
                      </TabPane>
                    </TabContent>
                  </Form>
                </CardBody>
              </Card>
            </Col>
          </Row>

          <Row>
            <Col xl={12}>
              <Card>
                <CardHeader>
                  <h4 className="card-title mb-0">Vertical nav Steps</h4>
                </CardHeader>
                <CardBody className="form-steps">
                  <form className="vertical-navs-step">
                    <Row className="gy-5">
                      <Col lg={3}>
                        <Nav
                          className="flex-column custom-nav nav-pills"
                        >
                          <NavItem>
                            <NavLink
                              href="#"
                              className={
                                (classnames({
                                  active: activeVerticalTab === 7,
                                  done: (activeVerticalTab <= 11 && activeVerticalTab > 7)
                                }))
                              }
                              onClick={() => {
                                toggleVerticalTab(7);
                              }}
                            >
                              <span className="step-title me-2">
                                <i className="ri-close-circle-fill step-icon me-2"></i>
                                Step 1
                              </span>
                              Billing Info
                            </NavLink>
                          </NavItem>
                          <NavItem>
                            <NavLink
                              href="#"
                              className={
                                (classnames({
                                  active: activeVerticalTab === 8,
                                  done: (activeVerticalTab <= 11 && activeVerticalTab >= 8)
                                }))
                              }
                              onClick={() => {
                                toggleVerticalTab(8);
                              }}
                            >
                              <span className="step-title me-2">
                                <i className="ri-close-circle-fill step-icon me-2"></i>
                                Step 2
                              </span>
                              Address
                            </NavLink>
                          </NavItem>
                          <NavItem>
                            <NavLink
                              href="#"
                              className={classnames({
                                active: activeVerticalTab === 9,
                                done: (activeVerticalTab <= 11 && activeVerticalTab >= 9)
                              })}
                              onClick={() => {
                                toggleVerticalTab(9);
                              }}
                            >
                              <span className="step-title me-2">
                                <i className="ri-close-circle-fill step-icon me-2"></i>
                                Step 3
                              </span>
                              Payment
                            </NavLink>
                          </NavItem>
                          <NavItem>
                            <NavLink
                              href="#"
                              className={classnames({
                                active: activeVerticalTab === 10,
                                done: (activeVerticalTab <= 11 && activeVerticalTab >= 10)
                              })}
                              onClick={() => {
                                toggleVerticalTab(10);
                              }}
                            >
                              <span className="step-title me-2">
                                <i className="ri-close-circle-fill step-icon me-2"></i>
                                Step 4
                              </span>
                              Finish
                            </NavLink>
                          </NavItem>
                        </Nav>
                      </Col>
                      <Col lg={6}>
                        <div className="px-lg-4">
                          <TabContent activeTab={activeVerticalTab}>
                            <TabPane tabId={7}>
                              <div>
                                <h5>Billing Info</h5>
                                <p className="text-muted">
                                  Fill all information below
                                </p>
                              </div>

                              <div>
                                <Row className="g-3">
                                  <Col sm={6}>
                                    <Label
                                      htmlFor="firstName"
                                      className="form-label"
                                    >
                                      First name
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="firstName"
                                      placeholder="Enter first name"
                                      defaultValue=""
                                      required
                                    />
                                    <div className="invalid-feedback">Please enter a first name</div>
                                  </Col>

                                  <Col sm={6}>
                                    <Label
                                      htmlFor="lastName"
                                      className="form-label"
                                    >
                                      Last name
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="lastName"
                                      placeholder="Enter last name"
                                      defaultValue=""
                                      required
                                    />
                                    <div className="invalid-feedback">Please enter a last name</div>
                                  </Col>

                                  <Col xs={12}>
                                    <Label
                                      htmlFor="username"
                                      className="form-label"
                                    >
                                      Username
                                    </Label>
                                    <div className="input-group">
                                      <span className="input-group-text">
                                        @
                                      </span>
                                      <Input
                                        type="text"
                                        className="form-control"
                                        id="username"
                                        placeholder="Username"
                                        required
                                      />
                                      <div className="invalid-feedback">Please enter a user name</div>
                                    </div>
                                  </Col>

                                  <Col xs={12}>
                                    <Label
                                      htmlFor="email"
                                      className="form-label"
                                    >
                                      Email{" "}
                                      <span className="text-muted">
                                        (Optional)
                                      </span>
                                    </Label>
                                    <Input
                                      type="email"
                                      className="form-control"
                                      id="email"
                                      placeholder="Enter Email"
                                    />
                                  </Col>
                                </Row>
                              </div>

                              <div className="d-flex align-items-start gap-3 mt-4">
                                <button
                                  type="button"
                                  className="btn btn-success btn-label right ms-auto nexttab nexttab"
                                  onClick={() => {
                                    toggleVerticalTab(activeVerticalTab + 1);
                                  }}

                                  data-nexttab="v-pills-bill-address-tab">
                                  <i className="ri-arrow-right-line label-icon align-middle fs-16 ms-2">
                                  </i>Go to Shipping
                                </button>
                              </div>
                            </TabPane>

                            <TabPane tabId={8}>
                              <div>
                                <h5>Shipping Address</h5>
                                <p className="text-muted">
                                  Fill all information below
                                </p>
                              </div>

                              <div>
                                <Row className="g-3">
                                  <Col xs={12}>
                                    <Label
                                      htmlFor="address"
                                      className="form-label"
                                    >
                                      Address
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="address"
                                      placeholder="1234 Main St"
                                      required
                                    />
                                    <div className="invalid-feedback">Please enter a address</div>
                                  </Col>

                                  <Col xs={12}>
                                    <Label
                                      htmlFor="address2"
                                      className="form-label"
                                    >
                                      Address 2{" "}
                                      <span className="text-muted">
                                        (Optional)
                                      </span>
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="address2"
                                      placeholder="Apartment or suite"
                                    />
                                  </Col>

                                  <Col md={5}>
                                    <Label
                                      htmlFor="country"
                                      className="form-label"
                                    >
                                      Country
                                    </Label>
                                    <select
                                      className="form-select"
                                      id="country"
                                      required
                                    >
                                      <option defaultValue="">Choose...</option>
                                      <option>United States</option>
                                    </select>
                                    <div className="invalid-feedback">Please select a country</div>
                                  </Col>

                                  <Col md={4}>
                                    <Label
                                      htmlFor="state"
                                      className="form-label"
                                    >
                                      State
                                    </Label>
                                    <select className="form-select" id="state">
                                      <option defaultValue="">Choose...</option>
                                      <option>California</option>
                                    </select>
                                    <div className="invalid-feedback">Please select a state</div>
                                  </Col>

                                  <Col md={3}>
                                    <Label htmlFor="zip" className="form-label">
                                      Zip
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="zip"
                                      placeholder=""
                                    />
                                  </Col>
                                </Row>

                                <hr className="my-4 text-muted" />

                                <div className="form-check mb-2">
                                  <Input
                                    type="checkbox"
                                    className="form-check-input"
                                    id="same-address"
                                  />
                                  <Label
                                    className="form-check-label"
                                    htmlFor="same-address"
                                  >
                                    Shipping address is the same as my billing
                                    address
                                  </Label>
                                </div>

                                <div className="form-check">
                                  <Input
                                    type="checkbox"
                                    className="form-check-input"
                                    id="save-info"
                                  />
                                  <Label
                                    className="form-check-label"
                                    htmlFor="save-info"
                                  >
                                    Save this information for next time
                                  </Label>
                                </div>
                              </div>
                              <div className="d-flex align-items-start gap-3 mt-4">
                                <button
                                  type="button"
                                  className="btn btn-light btn-label previestab"
                                  onClick={() => {
                                    toggleVerticalTab(activeVerticalTab - 1);
                                  }}
                                >
                                  <i className="ri-arrow-left-line label-icon align-middle fs-16 me-2"></i>{" "}
                                  Back to Billing Info
                                </button>
                                <button
                                  type="button"
                                  className="btn btn-success btn-label right ms-auto nexttab"
                                  onClick={() => {
                                    toggleVerticalTab(activeVerticalTab + 1);
                                  }}
                                >
                                  <i className="ri-arrow-right-line label-icon align-middle fs-16 ms-2"></i>
                                  Go to Payment
                                </button>
                              </div>
                            </TabPane>

                            <TabPane tabId={9}>
                              <div>
                                <h5>Payment</h5>
                                <p className="text-muted">
                                  Fill all information below
                                </p>
                              </div>

                              <div>
                                <div className="my-3">
                                  <div className="form-check form-check-inline">
                                    <Input
                                      id="credit"
                                      name="paymentMethod"
                                      type="radio"
                                      className="form-check-input"
                                      defaultChecked
                                      required
                                    />
                                    <Label
                                      className="form-check-label"
                                      htmlFor="credit"
                                    >
                                      Credit card
                                    </Label>
                                  </div>
                                  <div className="form-check form-check-inline">
                                    <Input
                                      id="debit"
                                      name="paymentMethod"
                                      type="radio"
                                      className="form-check-input"
                                      required
                                    />
                                    <Label
                                      className="form-check-label"
                                      htmlFor="debit"
                                    >
                                      Debit card
                                    </Label>
                                  </div>
                                  <div className="form-check form-check-inline">
                                    <Input
                                      id="paypal"
                                      name="paymentMethod"
                                      type="radio"
                                      className="form-check-input"
                                      required
                                    />
                                    <Label
                                      className="form-check-label"
                                      htmlFor="paypal"
                                    >
                                      PayPal
                                    </Label>
                                  </div>
                                </div>

                                <Row className="gy-3">
                                  <Col md={12}>
                                    <Label
                                      htmlFor="cc-name"
                                      className="form-label"
                                    >
                                      Name on card
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="cc-name"
                                      placeholder=""
                                      required
                                    />
                                    <small className="text-muted">
                                      Full name as displayed on card
                                    </small>
                                    <div className="invalid-feedback">
                                      Name on card is required
                                    </div>
                                  </Col>

                                  <Col md={6}>
                                    <Label
                                      htmlFor="cc-number"
                                      className="form-label"
                                    >
                                      Credit card number
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="cc-number"
                                      placeholder=""
                                      required
                                    />
                                    <div className="invalid-feedback">
                                      Credit card number is required
                                    </div>
                                  </Col>

                                  <Col md={3}>
                                    <Label
                                      htmlFor="cc-expiration"
                                      className="form-label"
                                    >
                                      Expiration
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="cc-expiration"
                                      placeholder=""
                                      required
                                    />
                                    <div className="invalid-feedback">
                                      Expiration date required
                                    </div>
                                  </Col>

                                  <Col md={3}>
                                    <Label
                                      htmlFor="cc-cvv"
                                      className="form-label"
                                    >
                                      CVV
                                    </Label>
                                    <Input
                                      type="text"
                                      className="form-control"
                                      id="cc-cvv"
                                      placeholder=""
                                      required
                                    />
                                    <div className="invalid-feedback">
                                      Security code required
                                    </div>
                                  </Col>
                                </Row>
                              </div>

                              <div className="d-flex align-items-start gap-3 mt-4">
                                <button
                                  type="button"
                                  className="btn btn-light btn-label previestab"
                                  onClick={() => {
                                    toggleVerticalTab(activeVerticalTab - 1);
                                  }}
                                >
                                  <i className="ri-arrow-left-line label-icon align-middle fs-16 me-2"></i>{" "}
                                  Back to Shipping Info
                                </button>
                                <button
                                  type="button"
                                  className="btn btn-success btn-label right ms-auto nexttab"
                                  onClick={() => {
                                    toggleVerticalTab(activeVerticalTab + 1);
                                  }}
                                >
                                  <i className="ri-arrow-right-line label-icon align-middle fs-16 ms-2"></i>{" "}
                                  Order Complete
                                </button>
                              </div>
                            </TabPane>

                            <TabPane tabId={10}>
                              <div className="text-center pt-4 pb-2">
                                <div className="mb-4">
                                  <lord-icon
                                    src="https://cdn.lordicon.com/lupuorrc.json"
                                    trigger="loop"
                                    colors="primary:#6ada7d,secondary:#5ea3cb"
                                    style={{ width: "120px", height: "120px" }}
                                  ></lord-icon>
                                </div>
                                <h5>Your Order is Completed !</h5>
                                <p className="text-muted">
                                  You Will receive an order confirmation email
                                  with details of your order.
                                </p>
                              </div>
                            </TabPane>
                          </TabContent>
                        </div>
                      </Col>

                      <Col lg={3}>
                        <div className="d-flex justify-content-between align-items-center mb-3">
                          <h5 className="fs-14 text-primary mb-0">
                            <i className="ri-shopping-cart-fill align-middle me-2"></i>{" "}
                            Your cart
                          </h5>
                          <span className="badge bg-danger rounded-pill">
                            3
                          </span>
                        </div>
                        <ul className="list-group mb-3">
                          <li className="list-group-item d-flex justify-content-between lh-sm">
                            <div>
                              <h6 className="my-0">Product name</h6>
                              <small className="text-muted">
                                Brief description
                              </small>
                            </div>
                            <span className="text-muted">$12</span>
                          </li>
                          <li className="list-group-item d-flex justify-content-between lh-sm">
                            <div>
                              <h6 className="my-0">Second product</h6>
                              <small className="text-muted">
                                Brief description
                              </small>
                            </div>
                            <span className="text-muted">$8</span>
                          </li>
                          <li className="list-group-item d-flex justify-content-between lh-sm">
                            <div>
                              <h6 className="my-0">Third item</h6>
                              <small className="text-muted">
                                Brief description
                              </small>
                            </div>
                            <span className="text-muted">$5</span>
                          </li>
                          <li className="list-group-item d-flex justify-content-between bg-light">
                            <div className="text-success">
                              <h6 className="my-0">Discount code</h6>
                              <small>−$5 Discount</small>
                            </div>
                            <span className="text-success">−$5</span>
                          </li>
                          <li className="list-group-item d-flex justify-content-between">
                            <span>Total (USD)</span>
                            <strong>$20</strong>
                          </li>
                        </ul>
                      </Col>
                    </Row>
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

export default FormWizard;
