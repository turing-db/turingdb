import React, { useState } from "react";

//Import Breadcrumb
import BreadCrumb from "../../Components/Common/BreadCrumb";

import {
  Container,
  Form,
  Row,
  Col,
  Card,
  CardBody,
  CardHeader,
  Nav,
  NavItem,
  NavLink,
  TabContent,
  TabPane,
  Modal,
  ModalFooter,
  ModalHeader,
  ModalBody,
  Label,
  Input,
} from "reactstrap";

import Select from "react-select";
import classnames from "classnames";
import { orderSummary } from "../../common/data/ecommerce";
import { Link } from "react-router-dom";

const EcommerceCheckout = () => {
  document.title="Checkout | Velzon - React Admin & Dashboard Template";


  const [selectedCountry, setselectedCountry] = useState(null);
  const [selectedState, setselectedState] = useState(null);
  const [activeTab, setactiveTab] = useState(1);
  const [passedSteps, setPassedSteps] = useState([1]);
  const [modal, setModal] = useState(false);
  const [deletemodal, setDeleteModal] = useState(false);

  const toggledeletemodal = () => {
    setDeleteModal(!deletemodal);
  };

  const togglemodal = () => {
    setModal(!modal);
  };

  function handleSelectCountry(selectedCountry) {
    setselectedCountry(selectedCountry);
  }

  function handleSelectState(selectedState) {
    setselectedState(selectedState);
  }

  function toggleTab(tab) {
    if (activeTab !== tab) {
      var modifiedSteps = [...passedSteps, tab];

      if (tab >= 1 && tab <= 4) {
        setactiveTab(tab);
        setPassedSteps(modifiedSteps);
      }
    }
  }

  const productState = [
    {
      options: [
        { label: "Select State...", value: "Select State" },
        { label: "Alabama", value: "Alabama" },
        { label: "Alaska", value: "Alaska" },
        { label: "American Samoa", value: "American Samoa" },
        { label: "California", value: "California" },
        { label: "Colorado", value: "Colorado" },
        { label: "District Of Columbia", value: "District Of Columbia" },
        { label: "Florida", value: "Florida" },
        { label: "Georgia", value: "Georgia" },
        { label: "Guam", value: "Guam" },
        { label: "Hawaii", value: "Hawaii" },
        { label: "Idaho", value: "Idaho" },
        { label: "Kansas", value: "Kansas" },
        { label: "Louisiana", value: "Louisiana" },
        { label: "Montana", value: "Montana" },
        { label: "Nevada", value: "Nevada" },
        { label: "New Jersey", value: "New Jersey" },
        { label: "New Mexico", value: "New Mexico" },
        { label: "New York", value: "New York" },
      ],
    },
  ];

  const productCountry = [
    {
      options: [
        { label: "Select Country...", value: "Select Country" },
        { label: "United States", value: "United States" },
      ],
    },
  ];

  return (
    <React.Fragment>
      <div className="page-content">       
        <Container fluid>
          <BreadCrumb title="Checkout" pageTitle="Ecommerce" />

          <Row>
            <Col xl="8">
              <Card>
                <CardBody className="checkout-tab">
                  <Form action="#">
                    <div className="step-arrow-nav mt-n3 mx-n3 mb-3">
                      <Nav
                        className="nav-pills nav-justified custom-nav"
                        role="tablist"
                      >
                        <NavItem role="presentation">
                          <NavLink href="#"
                            className={classnames({ active: activeTab === 1, done: (activeTab <= 4 && activeTab >= 0) }, "p-3 fs-15")}
                            onClick={() => { toggleTab(1); }}
                          >
                            <i className="ri-user-2-line fs-16 p-2 bg-soft-primary text-primary rounded-circle align-middle me-2"></i>
                            Personal Info
                          </NavLink>
                        </NavItem>
                        <NavItem role="presentation">
                          <NavLink href="#"
                            className={classnames({ active: activeTab === 2, done: activeTab <= 4 && activeTab > 1 }, "p-3 fs-15")}
                            onClick={() => { toggleTab(2); }}
                          >
                            <i className="ri-truck-line fs-16 p-2 bg-soft-primary text-primary rounded-circle align-middle me-2"></i>
                            Shipping Info
                          </NavLink>
                        </NavItem>
                        <NavItem role="presentation">
                          <NavLink href="#"
                            className={classnames({ active: activeTab === 3, done: activeTab <= 4 && activeTab > 2 }, "p-3 fs-15")}
                            onClick={() => { toggleTab(3); }}
                          >
                            <i className="ri-bank-card-line fs-16 p-2 bg-soft-primary text-primary rounded-circle align-middle me-2"></i>
                            Payment Info
                          </NavLink>
                        </NavItem>
                        <NavItem role="presentation">
                          <NavLink href="#"
                            className={classnames({ active: activeTab === 4, done: activeTab <= 4 && activeTab > 3 }, "p-3 fs-15")}
                            onClick={() => { toggleTab(4); }}
                          >
                            <i className="ri-checkbox-circle-line fs-16 p-2 bg-soft-primary text-primary rounded-circle align-middle me-2"></i>
                            Finish
                          </NavLink>
                        </NavItem>
                      </Nav>
                    </div>

                    <TabContent activeTab={activeTab}>
                      <TabPane tabId={1} id="pills-bill-info">
                        <div>
                          <h5 className="mb-1">Billing Information</h5>
                          <p className="text-muted mb-4">
                            Please fill all information below
                          </p>
                        </div>

                        <div>
                          <Row>
                            <Col sm={6}>
                              <div className="mb-3">
                                <Label
                                  htmlFor="billinginfo-firstName"
                                  className="form-label"
                                >
                                  First Name
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="billinginfo-firstName"
                                  placeholder="Enter first name"
                                />
                              </div>
                            </Col>

                            <Col sm={6}>
                              <div className="mb-3">
                                <Label
                                  htmlFor="billinginfo-lastName"
                                  className="form-label"
                                >
                                  Last Name
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="billinginfo-lastName"
                                  placeholder="Enter last name"
                                />
                              </div>
                            </Col>
                          </Row>

                          <Row>
                            <Col sm={6}>
                              <div className="mb-3">
                                <Label
                                  htmlFor="billinginfo-email"
                                  className="form-label"
                                >
                                  Email
                                  <span className="text-muted">(Optional)</span>
                                </Label>
                                <Input
                                  type="email"
                                  className="form-control"
                                  id="billinginfo-email"
                                  placeholder="Enter email"
                                />
                              </div>
                            </Col>

                            <Col sm={6}>
                              <div className="mb-3">
                                <Label
                                  htmlFor="billinginfo-phone"
                                  className="form-label"
                                >
                                  Phone
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="billinginfo-phone"
                                  placeholder="Enter phone no."
                                />
                              </div>
                            </Col>
                          </Row>

                          <div className="mb-3">
                            <Label
                              htmlFor="billinginfo-address"
                              className="form-label"
                            >
                              Address
                            </Label>
                            <textarea
                              className="form-control"
                              id="billinginfo-address"
                              placeholder="Enter address"
                              rows="3"
                            ></textarea>
                          </div>

                          <Row>
                            <Col md={4}>
                              <div className="mb-3">
                                <Label htmlFor="country" className="form-label">
                                  Country
                                </Label>
                                <Select
                                  value={selectedCountry}
                                  onChange={() => {
                                    handleSelectCountry();
                                  }}
                                  options={productCountry}
                                  id="country"
                                ></Select>
                              </div>
                            </Col>

                            <Col md={4}>
                              <div className="mb-3">
                                <Label htmlFor="state" className="form-label">
                                  State
                                </Label>
                                <Select
                                  id="state"
                                  value={selectedState}
                                  onChange={() => {
                                    handleSelectState();
                                  }}
                                  options={productState}
                                ></Select>
                              </div>
                            </Col>

                            <Col md={4}>
                              <div className="mb-3">
                                <Label htmlFor="zip" className="form-label">
                                  Zip Code
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="zip"
                                  placeholder="Enter zip code"
                                />
                              </div>
                            </Col>
                          </Row>

                          <div className="d-flex align-items-start gap-3 mt-3">
                            <button
                              type="button"
                              className="btn btn-primary btn-label right ms-auto nexttab"
                              onClick={() => {
                                toggleTab(activeTab + 1);
                              }}
                            >
                              <i className="ri-truck-line label-icon align-middle fs-16 ms-2"></i>
                              Proceed to Shipping
                            </button>
                          </div>
                        </div>
                      </TabPane>

                      <TabPane tabId={2}>
                        <div>
                          <h5 className="mb-1">Shipping Information</h5>
                          <p className="text-muted mb-4">
                            Please fill all information below
                          </p>
                        </div>

                        <div className="mt-4">
                          <div className="d-flex align-items-center mb-2">
                            <div className="flex-grow-1">
                              <h5 className="fs-15 mb-0">Saved Address</h5>
                            </div>
                            <div className="flex-shrink-0">
                              <button
                                type="button"
                                className="btn btn-sm btn-success mb-3"
                                onClick={togglemodal}
                              >
                                Add Address
                              </button>
                            </div>
                          </div>
                          <Row className="gy-3">
                            <Col lg={4} sm={6}>
                              <div className="form-check card-radio">
                                <Input
                                  id="shippingAddress01"
                                  name="shippingAddress"
                                  type="radio"
                                  className="form-check-input"
                                  defaultChecked
                                />
                                <Label
                                  className="form-check-label"
                                  htmlFor="shippingAddress01"
                                >
                                  <span className="mb-4 fw-semibold d-block text-muted text-uppercase">
                                    Home Address
                                  </span>

                                  <span className="fs-15 mb-2 d-block">
                                    Marcus Alfaro
                                  </span>
                                  <span className="text-muted fw-normal text-wrap mb-1 d-block">
                                    4739 Bubby Drive Austin, TX 78729
                                  </span>
                                  <span className="text-muted fw-normal d-block">
                                    Mo. 012-345-6789
                                  </span>
                                </Label>
                              </div>
                              <div className="d-flex flex-wrap p-2 py-1 bg-light rounded-bottom border mt-n1">
                                <div>
                                  <Link
                                    to="#"
                                    className="d-block text-body p-1 px-2"
                                    onClick={togglemodal}
                                  >
                                    <i className="ri-pencil-fill text-muted align-bottom me-1"></i>
                                    Edit
                                  </Link>
                                </div>
                                <div>
                                  <Link
                                    to="#"
                                    className="d-block text-body p-1 px-2"
                                    onClick={toggledeletemodal}
                                  >
                                    <i className="ri-delete-bin-fill text-muted align-bottom me-1"></i>
                                    Remove
                                  </Link>
                                </div>
                              </div>
                            </Col>
                            <Col lg={4} sm={6}>
                              <div className="form-check card-radio">
                                <Input
                                  id="shippingAddress02"
                                  name="shippingAddress"
                                  type="radio"
                                  className="form-check-input"
                                />
                                <Label
                                  className="form-check-label"
                                  htmlFor="shippingAddress02"
                                >
                                  <span className="mb-4 fw-semibold d-block text-muted text-uppercase">
                                    Office Address
                                  </span>

                                  <span className="fs-15 mb-2 d-block">
                                    James Honda
                                  </span>
                                  <span className="text-muted fw-normal text-wrap mb-1 d-block">
                                    1246 Virgil Street Pensacola, FL 32501
                                  </span>
                                  <span className="text-muted fw-normal d-block">
                                    Mo. 012-345-6789
                                  </span>
                                </Label>
                              </div>
                              <div className="d-flex flex-wrap p-2 py-1 bg-light rounded-bottom border mt-n1">
                                <div>
                                  <Link
                                    to="#"
                                    className="d-block text-body p-1 px-2"
                                    onClick={togglemodal}
                                  >
                                    <i className="ri-pencil-fill text-muted align-bottom me-1"></i>
                                    Edit
                                  </Link>
                                </div>
                                <div>
                                  <Link
                                    to="#"
                                    className="d-block text-body p-1 px-2"
                                    onClick={toggledeletemodal}
                                  >
                                    <i className="ri-delete-bin-fill text-muted align-bottom me-1"></i>
                                    Remove
                                  </Link>
                                </div>
                              </div>
                            </Col>
                          </Row>

                          <div className="mt-4">
                            <h5 className="fs-14 mb-3">Shipping Method</h5>

                            <Row className="g-4">
                              <Col lg={6}>
                                <div className="form-check card-radio">
                                  <Input
                                    id="shippingMethod01"
                                    name="shippingMethod"
                                    type="radio"
                                    className="form-check-input"
                                  />
                                  <Label
                                    className="form-check-label"
                                    htmlFor="shippingMethod01"
                                  >
                                    <span className="fs-21 float-end mt-2 text-wrap d-block fw-semibold">
                                      Free
                                    </span>
                                    <span className="fs-15 mb-1 text-wrap d-block">
                                      Free Delivery
                                    </span>
                                    <span className="text-muted fw-normal text-wrap d-block">
                                      Expected Delivery 3 to 5 Days
                                    </span>
                                  </Label>
                                </div>
                              </Col>
                              <Col lg={6}>
                                <div className="form-check card-radio">
                                  <Input
                                    id="shippingMethod02"
                                    name="shippingMethod"
                                    type="radio"
                                    className="form-check-input"
                                    defaultChecked
                                  />
                                  <Label
                                    className="form-check-label"
                                    htmlFor="shippingMethod02"
                                  >
                                    <span className="fs-21 float-end mt-2 text-wrap d-block fw-semibold">
                                      $24.99
                                    </span>
                                    <span className="fs-15 mb-1 text-wrap d-block">
                                      Express Delivery
                                    </span>
                                    <span className="text-muted fw-normal text-wrap d-block">
                                      Delivery within 24hrs.
                                    </span>
                                  </Label>
                                </div>
                              </Col>
                            </Row>
                          </div>
                        </div>

                        <div className="d-flex align-items-start gap-3 mt-4">
                          <button
                            type="button"
                            className="btn btn-light btn-label previestab"
                            onClick={() => {
                              toggleTab(activeTab - 1);
                            }}
                          >
                            <i className="ri-arrow-left-line label-icon align-middle fs-16 me-2"></i>
                            Back to Personal Info
                          </button>
                          <button
                            type="button"
                            className="btn btn-primary btn-label right ms-auto nexttab"
                            onClick={() => {
                              toggleTab(activeTab + 1);
                            }}
                          >
                            <i className="ri-bank-card-line label-icon align-middle fs-16 ms-2"></i>
                            Continue to Payment
                          </button>
                        </div>
                      </TabPane>

                      <TabPane tabId={3}>
                        <div>
                          <h5 className="mb-1">Payment Selection</h5>
                          <p className="text-muted mb-4">
                            Please select and enter your billing information
                          </p>
                        </div>

                        <Row className="g-4">
                          <Col lg={4} sm={6}>
                            <div>
                              <div className="form-check card-radio">
                                <Input
                                  id="paymentMethod01"
                                  name="paymentMethod"
                                  type="radio"
                                  className="form-check-input"
                                />
                                <Label
                                  className="form-check-label"
                                  htmlFor="paymentMethod01"
                                >
                                  <span className="fs-16 text-muted me-2">
                                    <i className="ri-paypal-fill align-bottom"></i>
                                  </span>
                                  <span className="fs-15 text-wrap">
                                    Paypal
                                  </span>
                                </Label>
                              </div>
                            </div>
                          </Col>
                          <Col lg={4} sm={6}>
                            <div>
                              <div className="form-check card-radio">
                                <Input
                                  id="paymentMethod02"
                                  name="paymentMethod"
                                  type="radio"
                                  className="form-check-input"
                                  defaultChecked
                                />
                                <Label
                                  className="form-check-label"
                                  htmlFor="paymentMethod02"
                                >
                                  <span className="fs-16 text-muted me-2">
                                    <i className="ri-bank-card-fill align-bottom"></i>
                                  </span>
                                  <span className="fs-15 text-wrap">
                                    Credit / Debit Card
                                  </span>
                                </Label>
                              </div>
                            </div>
                          </Col>

                          <Col lg={4} sm={6}>
                            <div>
                              <div className="form-check card-radio">
                                <Input
                                  id="paymentMethod03"
                                  name="paymentMethod"
                                  type="radio"
                                  className="form-check-input"
                                />
                                <Label
                                  className="form-check-label"
                                  htmlFor="paymentMethod03"
                                >
                                  <span className="fs-16 text-muted me-2">
                                    <i className="ri-money-dollar-box-fill align-bottom"></i>
                                  </span>
                                  <span className="fs-15 text-wrap">
                                    Cash on Delivery
                                  </span>
                                </Label>
                              </div>
                            </div>
                          </Col>
                        </Row>

                        <div
                          className="collapse show"
                          id="paymentmethodCollapse"
                        >
                          <Card className="p-4 border shadow-none mb-0 mt-4">
                            <Row className="gy-3">
                              <Col md={12}>
                                <Label htmlFor="cc-name" className="form-label">
                                  Name on card
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="cc-name"
                                  placeholder="Enter name"
                                />
                                <small className="text-muted">
                                  Full name as displayed on card
                                </small>
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
                                  placeholder="xxxx xxxx xxxx xxxx"
                                />
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
                                  placeholder="MM/YY"
                                />
                              </Col>

                              <Col md={3}>
                                <Label htmlFor="cc-cvv" className="form-label">
                                  CVV
                                </Label>
                                <Input
                                  type="text"
                                  className="form-control"
                                  id="cc-cvv"
                                  placeholder="xxx"
                                />
                              </Col>
                            </Row>
                          </Card>
                          <div className="text-muted mt-2 fst-italic">
                            <i
                              data-feather="lock"
                              className="text-muted icon-xs"
                            ></i>{" "}
                            Your transaction is secured with SSL encryption
                          </div>
                        </div>

                        <div className="d-flex align-items-start gap-3 mt-4">
                          <button
                            type="button"
                            className="btn btn-light btn-label previestab"
                            onClick={() => {
                              toggleTab(activeTab - 1);
                            }}
                          >
                            <i className="ri-arrow-left-line label-icon align-middle fs-16 me-2"></i>
                            Back to Shipping
                          </button>
                          <button
                            type="button"
                            className="btn btn-primary btn-label right ms-auto nexttab"
                            onClick={() => {
                              toggleTab(activeTab + 1);
                            }}
                          >
                            <i className="ri-shopping-basket-line label-icon align-middle fs-16 ms-2"></i>
                            Complete Order
                          </button>
                        </div>
                      </TabPane>

                      <TabPane tabId={4} id="pills-finish">
                        <div className="text-center py-5">
                          <div className="mb-4">
                            <lord-icon
                              src="https://cdn.lordicon.com/lupuorrc.json"
                              trigger="loop"
                              colors="primary:#0ab39c,secondary:#405189"
                              style={{ width: "120px", height: "120px" }}
                            ></lord-icon>
                          </div>
                          <h5>Thank you ! Your Order is Completed !</h5>
                          <p className="text-muted">
                            You will receive an order confirmation email with
                            details of your order.
                          </p>

                          <h3 className="fw-semibold">
                            Order ID:{" "}
                            <a
                              href="apps-ecommerce-order-details"
                              className="text-decoration-underline"
                            >
                              VZ2451
                            </a>
                          </h3>
                        </div>
                      </TabPane>
                    </TabContent>
                  </Form>
                </CardBody>
              </Card>
            </Col>

            <Col xl={4}>
              <Card>
                <CardHeader>
                  <div className="d-flex">
                    <div className="flex-grow-1">
                      <h5 className="card-title mb-0">Order Summary</h5>
                    </div>
                  </div>
                </CardHeader>
                <CardBody>
                  <div className="table-responsive table-card">
                    <table className="table table-borderless align-middle mb-0">
                      <thead className="table-light text-muted">
                        <tr>
                          <th style={{ width: "90px" }} scope="col">
                            Product
                          </th>
                          <th scope="col">Product Info</th>
                          <th scope="col" className="text-end">
                            Price
                          </th>
                        </tr>
                      </thead>
                      <tbody>
                        {orderSummary.map((product, key) => (
                          <React.Fragment key={key}>
                            <tr>
                              <td>
                                <div className="avatar-md bg-light rounded p-1">
                                  <img
                                    src={product.img}
                                    alt=""
                                    className="img-fluid d-block"
                                  />
                                </div>
                              </td>
                              <td>
                                <h5 className="fs-15">
                                  <Link
                                    to="/apps-ecommerce-product-details"
                                    className="text-dark"
                                  >
                                    {product.name}
                                  </Link>
                                </h5>
                                <p className="text-muted mb-0">
                                  $ {product.price} x {product.quantity}
                                </p>
                              </td>
                              <td className="text-end">$ {product.total}</td>
                            </tr>
                          </React.Fragment>
                        ))}

                        <tr>
                          <td className="fw-semibold" colSpan="2">
                            Sub Total :
                          </td>
                          <td className="fw-semibold text-end">$ 359.96</td>
                        </tr>
                        <tr>
                          <td colSpan="2">
                            Discount{" "}
                            <span className="text-muted">(VELZON15)</span>:{" "}
                          </td>
                          <td className="text-end">- $ 50.00</td>
                        </tr>
                        <tr>
                          <td colSpan="2">Shipping Charge :</td>
                          <td className="text-end">$ 24.99</td>
                        </tr>
                        <tr>
                          <td colSpan="2">Estimated Tax (12%): </td>
                          <td className="text-end">$ 18.20</td>
                        </tr>
                        <tr className="table-active">
                          <th colSpan="2">Total (USD) :</th>
                          <td className="text-end">
                            <span className="fw-semibold">$353.15</span>
                          </td>
                        </tr>
                      </tbody>
                    </table>
                  </div>
                </CardBody>
              </Card>
            </Col>
          </Row>
        </Container>
      </div>
      {/* modal Delete Address */}
      <Modal
        isOpen={deletemodal}
        role="dialog"
        autoFocus={true}
        centered
        id="removeItemModal"
        toggle={toggledeletemodal}
      >
        <ModalHeader toggle={() => {
          setDeleteModal(!deletemodal);
        }}>
        </ModalHeader>
        <ModalBody>
          <div className="mt-2 text-center">
            <lord-icon
              src="https://cdn.lordicon.com/gsqxdxog.json"
              trigger="loop"
              colors="primary:#f7b84b,secondary:#fa896b"
              style={{ width: "100px", height: "100px" }}
            ></lord-icon>
            <div className="mt-4 pt-2 fs-15 mx-4 mx-sm-5">
              <h4>Are you Sure ?</h4>
              <p className="text-muted mx-4 mb-0">
                Are you Sure You want to Remove this Address ?
              </p>
            </div>
          </div>
          <div className="d-flex gap-2 justify-content-center mt-4 mb-2">
            <button
              type="button"
              className="btn w-sm btn-light"
              onClick={() => {
                setDeleteModal(!deletemodal);
              }}
            >
              Close
            </button>
            <button type="button" className="btn w-sm btn-danger" onClick={() => {
              setDeleteModal(!deletemodal);
            }}>
              Yes, Delete It!
            </button>
          </div>
        </ModalBody>
      </Modal>

      {/* modal Add Address */}
      <Modal
        isOpen={modal}
        role="dialog"
        autoFocus={true}
        centered
        id="addAddressModal"
        toggle={togglemodal}
      >
        <ModalHeader
          toggle={() => {
            setModal(!modal);
          }}
        >
          <h5 className="modal-title" id="addAddressModalLabel">
            Address
          </h5>
        </ModalHeader>
        <ModalBody>
          <div>
            <div className="mb-3">
              <Label for="addaddress-Name" className="form-label">
                Name
              </Label>
              <Input
                type="text"
                className="form-control"
                id="addaddress-Name"
                placeholder="Enter Name"
              />
            </div>

            <div className="mb-3">
              <Label for="addaddress-textarea" className="form-label">
                Address
              </Label>
              <textarea
                className="form-control"
                id="addaddress-textarea"
                placeholder="Enter Address"
                rows="2"
              ></textarea>
            </div>

            <div className="mb-3">
              <Label for="addaddress-Name" className="form-label">
                Phone
              </Label>
              <Input
                type="text"
                className="form-control"
                id="addaddress-Name"
                placeholder="Enter Phone No."
              />
            </div>

            <div className="mb-3">
              <Label for="state" className="form-label">
                Address Type
              </Label>
              <select className="form-select" id="state" data-plugin="choices">
                <option value="homeAddress">Home (7am to 10pm)</option>
                <option value="officeAddress">Office (11am to 7pm)</option>
              </select>
            </div>
          </div>
        </ModalBody>
        <ModalFooter>
          <button
            type="button"
            className="btn btn-light"
            onClick={() => {
              setModal(!modal);
            }}
          >
            Close
          </button>
          <button
            type="button"
            className="btn btn-success"
            onClick={() => {
              setModal(!modal);
            }}
          >
            Save
          </button>
        </ModalFooter>
      </Modal>
    </React.Fragment>
  );
};

export default EcommerceCheckout;
