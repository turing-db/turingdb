import React, { useEffect, useState } from "react";

import {
  CardBody,
  Row,
  Col,
  Card,
  Container,
  Form,
  Input,
  Label,
  Table,
  FormFeedback,
} from "reactstrap";

import { Link, useNavigate } from "react-router-dom";
import Flatpickr from "react-flatpickr";

import BreadCrumb from "../../Components/Common/BreadCrumb";
import Select from "react-select";

import logoDark from "../../assets/images/logo-dark.png";
import logoLight from "../../assets/images/logo-light.png";

//formik
import { useFormik } from "formik";
import * as Yup from "yup";

//redux
import { useDispatch } from "react-redux";
import { addNewInvoice as onAddNewInvoice } from "../../slices/thunks";

const InvoiceCreate = () => {
  const dispatch = useDispatch();
  const history = useNavigate();

  const [ispaymentDetails, setispaymentDetails] = useState(null);
  const [isCurrency, setisCurrency] = useState("$");

  function handleispaymentDetails(ispaymentDetails) {
    setispaymentDetails(ispaymentDetails);
  }

  function handleisCurrency(isCurrency) {
    setisCurrency(isCurrency);
  }

  const paymentdetails = [
    {
      options: [
        { label: "Payment Method", value: "Payment Method" },
        { label: "Mastercard", value: "Mastercard" },
        { label: "Credit Card", value: "Credit Card" },
        { label: "Visa", value: "Visa" },
        { label: "Paypal", value: "Paypal" },
      ],
    },
  ];

  const allstatus = [
    {
      options: [
        { label: "Select Payment Status", value: "Select Payment Status" },
        { label: "Paid", value: "Paid" },
        { label: "Unpaid", value: "Unpaid" },
        { label: "Refund", value: "Refund" },
      ],
    },
  ];

  const allcurrency = [
    {
      options: [
        { label: "$", value: "($)" },
        { label: "£", value: "(£)" },
        { label: "₹", value: "(₹)" },
        { label: "€", value: "(€)" },
      ],
    },
  ];

  const [count, setCount] = useState(0);
  const [rate, setRate] = useState(0);
  const [tax, setTax] = useState(0);
  const [dis, setDis] = useState(0);
  const [charge, setCharge] = useState(0);

  useEffect(() => {
    let tax = (0.125 * rate * count);
    let dis = (0.15 * rate * count);

    if ((rate && count) && isNaN !== 0) {
      setCharge(65);
    } else {
      setCharge(0);

    }
    setTax(tax);
    setDis(dis);
  }, [rate, count]);

  const dateFormat = () => {
    let d = new Date(),
      months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    return ((d.getDate() + ' ' + months[d.getMonth()] + ', ' + d.getFullYear()).toString());
  };

  const [date, setDate] = useState(dateFormat());

  const dateformate = (e) => {
    const date = e.toString().split(" ");
    const joinDate = (date[2] + " " + date[1] + ", " + date[3]).toString();
    setDate(joinDate);
  };



  document.title = "Create Invoice | Velzon - React Admin & Dashboard Template";

  const validation = useFormik({
    enableReinitialize: true,

    initialValues: {
      postalcode: "",
      registration: "",
      email: "",
      website: "",
      contact: "",
      invoiceId: "",
      date: "",
      name: "",
      status: "",
      country: "",
      amount: "",
      billing_address: "",
      billing_phone: "",
      billing_taxno: "",
      shipping_name: "",
      shipping_address: "",
      shipping_phone: "",
      shipping_taxno: "",
      product_name: "",
    },
    validationSchema: Yup.object({
      postalcode: Yup.string().required("This field is required"),
      registration: Yup.string().required("Please Enter a registration no"),
      email: Yup.string().required("Please Enter a Email"),
      website: Yup.string().required("Please Enter a website"),
      contact: Yup.string().required("Please Enter a contact number"),
      invoiceId: Yup.string().required("This field is required"),
      name: Yup.string().required("Please Enter a Full name"),
      billing_address: Yup.string().required("Please Enter a Address"),
      billing_phone: Yup.string().required("Please Enter a Phone Number"),
      billing_taxno: Yup.string().required("Please Enter a tax Number"),
      shipping_name: Yup.string().required("Please Enter a Full name"),
      shipping_address: Yup.string().required("Please Enter a Address"),
      shipping_phone: Yup.string().required("Please Enter a Phone Number"),
      shipping_taxno: Yup.string().required("Please enter a tax Number"),
      product_name: Yup.string().required("Please Enter a product Name"),
    }),
    onSubmit: (values) => {
      const newInvoice = {
        _id: (Math.floor(Math.random() * (30 - 20)) + 20).toString(),
        postalcode: values.postalcode,
        registration: values.registration,
        email: values.email,
        website: values.website,
        contact: values.contact,
        invoiceId: values.invoiceId,
        date: date,
        name: values.name,
        status: values.status,
        country: "United States of America",
        amount: Math.round(rate * count + tax + charge - dis),
        billing_address: values.billing_address,
        billing_phone: values.billing_phone,
        billing_taxno: values.billing_taxno,
        shipping_name: values.shipping_name,
        shipping_address: values.shipping_address,
        shipping_phone: values.shipping_phone,
        shipping_taxno: values.shipping_taxno,
        product_name: values.product_name,
      };
      dispatch(onAddNewInvoice(newInvoice));
      history("/apps-invoices-list");
      validation.resetForm();
    },
  });

  return (
    <div className="page-content">
      <Container fluid>
        <BreadCrumb title="Create Invoice" pageTitle="Invoices" />
        <Row className="justify-content-center">
          <Col xxl={9}>
            <Card>
              <Form
                onSubmit={(e) => {
                  e.preventDefault();
                  validation.handleSubmit();
                  return false;
                }}
                className="needs-validation"
                id="invoice_form"
              >
                <CardBody className="border-bottom border-bottom-dashed p-4">
                  <Row>
                    <Col lg={4}>
                      <div className="profile-user mx-auto  mb-3">
                        <Input
                          id="profile-img-file-input"
                          type="file"
                          className="profile-img-file-input"
                        />
                        <Label for="profile-img-file-input" className="d-block">
                          <span
                            className="overflow-hidden border border-dashed d-flex align-items-center justify-content-center rounded"
                            style={{ height: "60px", width: "256px" }}
                          >
                            <img
                              src={logoDark}
                              className="card-logo card-logo-dark user-profile-image img-fluid"
                              alt="logo dark"
                            />
                            <img
                              src={logoLight}
                              className="card-logo card-logo-light user-profile-image img-fluid"
                              alt="logo light"
                            />
                          </span>
                        </Label>
                      </div>
                      <div>
                        <div>
                          <Label for="companyAddress">Address</Label>
                        </div>
                        <div className="mb-2">
                          <Input
                            type="textarea"
                            className="form-control bg-light border-0"
                            id="companyAddress"
                            rows="3"
                            placeholder="Company Address"
                          />
                          <div className="invalid-feedback">
                            Please enter a address
                          </div>
                        </div>
                        <div>
                          <Input
                            type="text"
                            className="form-control bg-light border-0"
                            id="companyaddpostalcode"
                            minLength="5"
                            maxLength="6"
                            name="postalcode"
                            value={validation.values.postalcode || ""}
                            onBlur={validation.handleBlur}
                            onChange={validation.handleChange}
                            placeholder="Enter Postal Code"
                            invalid={validation.errors.postalcode && validation.touched.postalcode ? true : false}
                          />
                          {validation.errors.postalcode && validation.touched.postalcode ? (
                            <FormFeedback type="invalid">{validation.errors.postalcode}</FormFeedback>
                          ) : null}
                          
                        </div>
                      </div>
                    </Col>
                    <Col lg={4} className="ms-auto">
                      <div className="mb-2">
                        <Input
                          type="text"
                          className="form-control bg-light border-0"
                          id="registrationNumber"
                          name="registration"
                          value={validation.values.registration || ""}
                          onBlur={validation.handleBlur}
                          onChange={validation.handleChange}
                          maxLength="12"
                          placeholder="Legal Registration No"
                          invalid={validation.errors.registration && validation.touched.registration ? true : false}
                        />
                        {validation.errors.registration && validation.touched.registration ? (
                          <FormFeedback type="invalid">{validation.errors.registration}</FormFeedback>
                        ) : null}
                      </div>
                      <div className="mb-2">
                        <Input
                          type="email"
                          className="form-control bg-light border-0"
                          id="companyEmail"
                          name="email"
                          value={validation.values.email || ""}
                          onBlur={validation.handleBlur}
                          onChange={validation.handleChange}
                          placeholder="Email Address"
                          invalid={validation.errors.email && validation.touched.email ? true : false}
                        />
                        {validation.errors.email && validation.touched.email ? (
                          <FormFeedback type="invalid">{validation.errors.email}</FormFeedback>
                        ) : null}
                        
                      </div>
                      <div className="mb-2">
                        <Input
                          type="text"
                          className="form-control bg-light border-0"
                          id="companyWebsite"
                          name="website"
                          value={validation.values.website || ""}
                          onBlur={validation.handleBlur}
                          onChange={validation.handleChange}
                          placeholder="Website"
                          invalid={validation.errors.website && validation.touched.website ? true : false}
                        />
                        {validation.errors.website && validation.touched.website ? (
                          <FormFeedback type="invalid">{validation.errors.website}</FormFeedback>
                        ) : null}
                        
                      </div>
                      <div>
                        <Input
                          type="text"
                          className="form-control bg-light border-0"
                          data-plugin="cleave-phone"
                          id="compnayContactno"
                          name="contact"
                          value={validation.values.contact || ""}
                          onBlur={validation.handleBlur}
                          onChange={validation.handleChange}
                          placeholder="Contact No"
                          invalid={validation.errors.contact && validation.touched.contact ? true : false}
                        />
                        {validation.errors.contact && validation.touched.contact ? (
                          <FormFeedback type="invalid">{validation.errors.contact}</FormFeedback>
                        ) : null}
                        
                      </div>
                    </Col>
                  </Row>
                </CardBody>
                <CardBody className="p-4">
                  <Row className="g-3">
                    <Col lg={3} sm={6}>
                      <Label for="invoicenoInput">Invoice No</Label>
                      <Input
                        type="text"
                        className="form-control bg-light border-0"
                        id="invoicenoInput"
                        name="invoiceId"
                        value={validation.values.invoiceId || ""}
                        onBlur={validation.handleBlur}
                        onChange={validation.handleChange}
                        placeholder="Invoice No"
                        invalid={validation.errors.invoiceId && validation.touched.invoiceId ? true : false}
                      />
                      {validation.errors.invoiceId && validation.touched.invoiceId ? (
                        <FormFeedback type="invalid">{validation.errors.invoiceId}</FormFeedback>
                      ) : null}
                    </Col>
                    <Col lg={3} sm={6}>
                      <div>
                        <Label for="date-field">Date</Label>
                        <Flatpickr
                          name="date"
                          id="date-field"
                          className="form-control"
                          placeholder="Select a date"
                          options={{
                            altInput: true,
                            altFormat: "d M, Y",
                            dateFormat: "d M, Y",
                          }}
                          onChange={(e) =>
                            dateformate(e)
                          }
                          value={validation.values.date || ""}
                        />
                        {validation.touched.date && validation.errors.date ? (
                          <FormFeedback type="invalid">{validation.errors.date}</FormFeedback>
                        ) : null}
                      </div>
                    </Col>
                    <Col lg={3} sm={6}>
                      <Label for="choices-payment-status">Payment Status</Label>
                      <div className="input-light">

                        <Input
                          name="status"
                          type="select"
                          className="bg-light border-0"
                          id="choices-payment-status"
                          onChange={validation.handleChange}
                          onBlur={validation.handleBlur}
                          value={
                            validation.values.status || ""
                          }
                        >
                          {allstatus.map((item, key) => (
                            <React.Fragment key={key}>
                              {item.options.map((item, key) => (<option value={item.value} key={key}>{item.label}</option>))}
                            </React.Fragment>
                          ))}
                        </Input>
                        {validation.touched.status &&
                          validation.errors.status ? (
                          <FormFeedback type="invalid">
                            {validation.errors.status}
                          </FormFeedback>
                        ) : null}
                      
                      </div>
                    </Col>
                    <Col lg={3} sm={6}>
                      <div>
                        <Label for="totalamountInput">Total Amount</Label>
                        <Input
                          type="text"
                          className="form-control bg-light border-0"
                          id="totalamountInput"
                          placeholder="$0.00"
                          readOnly
                          value={"$" + Math.round(rate * count + tax + charge - dis)}
                        />
                      </div>
                    </Col>
                  </Row>
                </CardBody>
                <CardBody className="p-4 border-top border-top-dashed">
                  <Row>
                    <Col lg={4} sm={6}>
                      <div>
                        <Label
                          for="billingName"
                          className="text-muted text-uppercase fw-semibold"
                        >
                          Billing Address
                        </Label>
                      </div>
                      <div className="mb-2">
                        <Input
                          type="text"
                          className="form-control bg-light border-0"
                          id="billingName"
                          name="name"
                          value={validation.values.name || ""}
                          onBlur={validation.handleBlur}
                          onChange={validation.handleChange}
                          placeholder="Full Name"
                          invalid={validation.errors.name && validation.touched.name ? true : false}
                        />
                        {validation.errors.name && validation.touched.name ? (
                          <FormFeedback type="invalid">{validation.errors.name}</FormFeedback>
                        ) : null}
                        
                      </div>
                      <div className="mb-2">
                        <Input
                          type="textarea"
                          className="form-control bg-light border-0"
                          id="billingAddress"
                          name="billing_address"
                          value={validation.values.billing_address || ""}
                          onBlur={validation.handleBlur}
                          onChange={validation.handleChange}
                          rows="3"
                          placeholder="Address"
                          invalid={validation.errors.billing_address && validation.touched.billing_address ? true : false}
                        />
                        {validation.errors.billing_address && validation.touched.billing_address ? (
                          <FormFeedback type="invalid">{validation.errors.billing_address}</FormFeedback>
                        ) : null}
                        
                      </div>
                      <div className="mb-2">
                        <Input
                          type="text"
                          className="form-control bg-light border-0"
                          data-plugin="cleave-phone"
                          id="billingPhoneno"
                          name="billing_phone"
                          value={validation.values.billing_phone || ""}
                          onBlur={validation.handleBlur}
                          onChange={validation.handleChange}
                          placeholder="(123)456-7890"
                          invalid={validation.errors.billing_phone && validation.touched.billing_phone ? true : false}
                        />
                        {validation.errors.billing_phone && validation.touched.billing_phone ? (
                          <FormFeedback type="invalid">{validation.errors.billing_phone}</FormFeedback>
                        ) : null}
                       
                      </div>
                      <div className="mb-3">
                        <Input
                          type="text"
                          className="form-control bg-light border-0"
                          id="billingTaxno"
                          name="billing_taxno"
                          value={validation.values.billing_taxno || ""}
                          onBlur={validation.handleBlur}
                          onChange={validation.handleChange}
                          placeholder="Tax Number"
                          invalid={validation.errors.billing_taxno && validation.touched.billing_taxno ? true : false}
                        />
                        {validation.errors.billing_taxno && validation.touched.billing_taxno ? (
                          <FormFeedback type="invalid">{validation.errors.billing_taxno}</FormFeedback>
                        ) : null}
                        
                      </div>
                      <div className="form-check">
                        <Input
                          type="checkbox"
                          className="form-check-input"
                          id="same"
                          name="same"
                        />
                        <Label className="form-check-label" for="same">
                          Will your Billing and Shipping address same?
                        </Label>
                      </div>
                    </Col>
                    <Col sm={6} className="ms-auto">
                      <Row>
                        <Col lg={8}>
                          <div>
                            <Label
                              for="shippingName"
                              className="text-muted text-uppercase fw-semibold"
                            >
                              Shipping Address
                            </Label>
                          </div>
                          <div className="mb-2">
                            <Input
                              type="text"
                              className="form-control bg-light border-0"
                              id="shippingName"
                              name="shipping_name"
                              value={validation.values.shipping_name || ""}
                              onBlur={validation.handleBlur}
                              onChange={validation.handleChange}
                              placeholder="Full Name"
                              invalid={validation.errors.shipping_name && validation.touched.shipping_name ? true : false}
                            />
                            {validation.errors.shipping_name && validation.touched.shipping_name ? (
                              <FormFeedback type="invalid">{validation.errors.shipping_name}</FormFeedback>
                            ) : null}
                            
                          </div>
                          <div className="mb-2">
                            <Input
                              type="textarea"
                              className="form-control bg-light border-0"
                              id="shippingAddress"
                              rows="3"
                              name="shipping_address"
                              value={validation.values.shipping_address || ""}
                              onBlur={validation.handleBlur}
                              onChange={validation.handleChange}
                              placeholder="Address"
                              invalid={validation.errors.shipping_address && validation.touched.shipping_address ? true : false}
                            />
                            {validation.errors.shipping_address && validation.touched.shipping_address ? (
                              <FormFeedback type="invalid">{validation.errors.shipping_address}</FormFeedback>
                            ) : null}
                            
                          </div>
                          <div className="mb-2">
                            <Input
                              type="text"
                              className="form-control bg-light border-0"
                              data-plugin="cleave-phone"
                              id="shippingPhoneno"
                              name="shipping_phone"
                              value={validation.values.shipping_phone || ""}
                              onBlur={validation.handleBlur}
                              onChange={validation.handleChange}
                              placeholder="(123)456-7890"
                              invalid={validation.errors.shipping_phone && validation.touched.shipping_phone ? true : false}
                            />
                            {validation.errors.shipping_phone && validation.touched.shipping_phone ? (
                              <FormFeedback type="invalid">{validation.errors.shipping_phone}</FormFeedback>
                            ) : null}
                            
                          </div>
                          <div>
                            <Input
                              type="text"
                              className="form-control bg-light border-0"
                              id="shippingTaxno"
                              placeholder="Tax Number"
                              name="shipping_taxno"
                              value={validation.values.shipping_taxno || ""}
                              onBlur={validation.handleBlur}
                              onChange={validation.handleChange}
                              invalid={validation.errors.shipping_taxno && validation.touched.shipping_taxno ? true : false}
                            />
                            {validation.errors.shipping_taxno && validation.touched.shipping_taxno ? (
                              <FormFeedback type="invalid">{validation.errors.shipping_taxno}</FormFeedback>
                            ) : null}
                            
                          </div>
                        </Col>
                      </Row>
                    </Col>
                  </Row>
                </CardBody>
                <CardBody className="p-4">
                  <div className="table-responsive">
                    <Table className="invoice-table table-borderless table-nowrap mb-0">
                      <thead className="align-middle">
                        <tr className="table-active">
                          <th scope="col" style={{ width: "50px" }}>
                            #
                          </th>
                          <th scope="col">Product Details</th>
                          <th scope="col" style={{ width: "120px" }}>
                            <div className="d-flex currency-select input-light align-items-center">
                              Rate
                              <Select
                                defaultValue={isCurrency}
                                onChange={() => {
                                  handleisCurrency();
                                }}
                                options={allcurrency}
                                id="choices-payment-currency"
                                className="form-selectborder-0 bg-light"
                              />
                            </div>
                          </th>
                          <th scope="col" style={{ width: "120px" }}>
                            Quantity
                          </th>
                          <th
                            scope="col"
                            className="text-end"
                            style={{ width: "150px" }}
                          >
                            Amount
                          </th>
                          <th
                            scope="col"
                            className="text-end"
                            style={{ width: "105px" }}
                          ></th>
                        </tr>
                      </thead>
                      <tbody id="newlink">
                        <tr id="1" className="product">
                          <th scope="row" className="product-id">
                            1
                          </th>
                          <td className="text-start">
                            <div className="mb-2">
                              <Input
                                type="text"
                                className="form-control bg-light border-0"
                                id="productName-1"
                                placeholder="Product Name"
                                name="product_name"
                                value={validation.values.product_name || ""}
                                onBlur={validation.handleBlur}
                                onChange={validation.handleChange}
                                invalid={validation.errors.product_name && validation.touched.product_name ? true : false}
                              />
                              {validation.errors.product_name && validation.touched.product_name ? (
                                <FormFeedback type="invalid">{validation.errors.product_name}</FormFeedback>
                              ) : null}
                              
                            </div>
                            <Input
                              type="textarea"
                              className="form-control bg-light border-0"
                              id="productDetails-1"
                              rows="2"
                              placeholder="Product Details"
                            ></Input>
                          </td>
                          <td>
                            <Input
                              type="number"
                              className="form-control product-price bg-light border-0"
                              placeholder="0.00"
                              id="productRate-1" step="0.01"
                              onChange={(e) => {
                                setRate(e.target.value);
                              }}
                            />
                            <div className="invalid-feedback">
                              Please enter a rate
                            </div>
                          </td>
                          <td>


                            <div className="input-step">
                              <button type="button" className="minus" onClick={() => setCount(count > 0 ? (count - 1) : count)}>
                                –
                              </button>
                              <Input
                                type="number"
                                className="product-quantity"
                                id="product-qty-1"
                                value={count}
                                readOnly
                              />
                              <button type="button" className="plus" onClick={() => setCount(count + 1)}>
                                +
                              </button>
                            </div>


                          </td>
                          <td className="text-end">
                            <div>
                              <Input
                                type="text"
                                className="form-control bg-light border-0 product-line-price"
                                id="productPrice-1"
                                placeholder="$0.00"
                                value={"$" + rate * count}
                                readOnly
                              />
                            </div>
                          </td>
                          <td className="product-removal">
                            <Link to="#" className="btn btn-success">
                              Delete
                            </Link>
                          </td>
                        </tr>
                      </tbody>
                      <tbody>
                        <tr id="newForm" style={{ display: "none" }}><td className="d-none" colSpan="5"><p>Add New Form</p></td></tr>
                        <tr>
                          <td colSpan="5">
                            <Link
                              to="#"
                              className="btn btn-soft-secondary fw-medium"
                              id="add-item"
                            >
                              <i className="ri-add-fill me-1 align-bottom"></i>{" "}
                              Add Item
                            </Link>
                          </td>
                        </tr>
                        <tr className="border-top border-top-dashed mt-2">
                          <td colSpan="3"></td>
                          <td colSpan="2" className="p-0">
                            <Table className="table-borderless table-sm table-nowrap align-middle mb-0">
                              <tbody>
                                <tr>
                                  <th scope="row">Sub Total</th>
                                  <td style={{ width: "150px" }}>
                                    <Input
                                      type="text"
                                      className="form-control bg-light border-0"
                                      id="cart-subtotal"
                                      placeholder="$0.00"
                                      readOnly
                                      value={"$" + rate * count}
                                    />
                                  </td>
                                </tr>
                                <tr>
                                  <th scope="row">Estimated Tax (12.5%)</th>
                                  <td>
                                    <Input
                                      type="text"
                                      className="form-control bg-light border-0"
                                      id="cart-tax"
                                      placeholder="$0.00"
                                      readOnly
                                      value={"$" + tax}
                                    />
                                  </td>
                                </tr>
                                <tr>
                                  <th scope="row">
                                    Discount{" "}
                                    <small className="text-muted">
                                      (VELZON15)
                                    </small>
                                  </th>
                                  <td>
                                    <Input
                                      type="text"
                                      className="form-control bg-light border-0"
                                      id="cart-discount"
                                      placeholder="$0.00"
                                      readOnly
                                      value={"$" + dis}
                                    />
                                  </td>
                                </tr>
                                <tr>
                                  <th scope="row">Shipping Charge</th>
                                  <td>
                                    <Input
                                      type="text"
                                      className="form-control bg-light border-0"
                                      id="cart-shipping"
                                      placeholder="$0.00"
                                      readOnly
                                      value={"$" + charge}
                                    />
                                  </td>
                                </tr>
                                <tr className="border-top border-top-dashed">
                                  <th scope="row">Total Amount</th>
                                  <td>
                                    <Input
                                      type="text"
                                      className="form-control bg-light border-0"
                                      id="cart-total"
                                      placeholder="$0.00"
                                      readOnly
                                      value={"$" + Math.round(rate * count + tax + charge - dis)}
                                    />
                                  </td>
                                </tr>
                              </tbody>
                            </Table>
                          </td>
                        </tr>
                      </tbody>
                    </Table>
                  </div>
                  <Row className="mt-3">
                    <Col lg={4}>
                      <div className="mb-2">
                        <Label
                          for="choices-payment-type"
                          className="form-label text-muted text-uppercase fw-semibold"
                        >
                          Payment Details
                        </Label>
                        <div className="input-light">
                          <Select
                            value={ispaymentDetails}
                            onChange={() => {
                              handleispaymentDetails();
                            }}
                            options={paymentdetails}
                            name="choices-single-default"
                            id="idStatus"
                            className="bg-light border-0"
                          ></Select>
                        </div>
                      </div>
                      <div className="mb-2">
                        <Input
                          className="form-control bg-light border-0"
                          type="text"
                          id="cardholderName"
                          placeholder="Card Holder Name"
                        />
                      </div>
                      <div className="mb-2">
                        <Input
                          className="form-control bg-light border-0"
                          type="text"
                          id="cardNumber"
                          placeholder="xxxx xxxx xxxx xxxx"
                        />
                      </div>
                      <div>
                        <Input
                          className="form-control  bg-light border-0"
                          type="text"
                          id="amountTotalPay"
                          placeholder="$0.00"
                          readOnly
                        />
                      </div>
                    </Col>
                  </Row>
                  <div className="mt-4">
                    <Label
                      for="exampleFormControlTextarea1"
                      className="form-label text-muted text-uppercase fw-semibold"
                    >
                      NOTES
                    </Label>
                    <Input
                      type="textarea"
                      className="form-control alert alert-info"
                      id="exampleFormControlTextarea1"
                      placeholder="Notes"
                      rows="2"
                      defaultValue="All accounts are to be paid within 7 days from receipt of
                      invoice. To be paid by cheque or credit card or direct
                      payment online. If account is not paid within 7 days the
                      credits details supplied as confirmation of work
                      undertaken will be charged the agreed quoted fee noted
                      above."
                    />
                  </div>
                  <div className="hstack gap-2 justify-content-end d-print-none mt-4">
                    <button type="submit" className="btn btn-success">
                      <i className="ri-printer-line align-bottom me-1"></i> Save
                    </button>
                    <Link to="#" className="btn btn-primary">
                      <i className="ri-download-2-line align-bottom me-1"></i>{" "}
                      Download Invoice
                    </Link>
                    <Link to="#" className="btn btn-danger">
                      <i className="ri-send-plane-fill align-bottom me-1"></i>{" "}
                      Send Invoice
                    </Link>
                  </div>
                </CardBody>
              </Form>
            </Card>
          </Col>
        </Row>
      </Container>
    </div>
  );
};

export default InvoiceCreate;
