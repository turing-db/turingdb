import React, { useEffect, useState } from "react";
//Import Flatepicker
import Flatpickr from "react-flatpickr";

//Import Breadcrumb
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import {
  CardBody,
  CardHeader,
  Container,
  Card,
  Row,
  Col,
  Input,
  ModalHeader,
  Modal,
  ModalBody,
  Nav,
  NavItem,
  NavLink,
  TabContent,
  TabPane,
  Label,
  Form,
} from "reactstrap";
import classnames from "classnames";
import { Link } from "react-router-dom";
import { isEmpty } from "lodash";
import Select from "react-select";

//redux

import { useSelector, useDispatch } from "react-redux";

//Import actions
import { getSellers as onGetSellers } from "../../../slices/thunks";
import SellerChats from "./SellerChats";

const EcommerceSellers = () => {
  document.title = "Sellers | Velzon - React Admin & Dashboard Template";

  const dispatch = useDispatch();
  const [sellerList, setSellerList] = useState([]);
  const [modal, setModal] = useState(false);
  const [companyType, setcompanyType] = useState(null);

  const { sellers } = useSelector((state) => ({
    sellers: state.Ecommerce.sellers,
  }));

  useEffect(() => {
    setSellerList(sellers);
  }, [sellers]);

  useEffect(() => {
    dispatch(onGetSellers());
  }, [dispatch]);

  useEffect(() => {
    if (!isEmpty(sellers)) setSellerList(sellers);
  }, [sellers]);

  const toggle = () => {
    if (modal) {
      setModal(false);
    } else {
      setModal(true);
    }
  };

  //Tab
  const [activeTab, setActiveTab] = useState("1");
  const toggleTab = (tab) => {
    if (activeTab !== tab) {
      setActiveTab(tab);
    }
  };

  const companytypes = [
    {
      options: [
        { label: "Select type", value: "Select type" },
        { label: "All", value: "All" },
        { label: "Merchandising", value: "Merchandising" },
        { label: "Manufacturing", value: "Manufacturing" },
        { label: "Partnership", value: "Partnership" },
        { label: "Corporation", value: "Corporation" },
      ],
    },
  ];

  function handlecompanyType(companyType) {
    setcompanyType(companyType);
  }

  const category = (e) => {
    if (e === "All") {
      var filter = sellers.filter((item) => item.category !== e);
    } else {
      filter = sellers.filter((item) => item.category === e);
    }
    setSellerList(filter);
  };

  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="Sellers" pageTitle="Ecommerce" />
          <Card>
            <CardHeader className="border-0 rounded">
              <Row className="g-2">
                <Col xl={3}>
                  <div className="search-box">
                    <Input
                      type="text"
                      className="form-control search"
                      placeholder="Search for sellers & owner name or something..."
                    />{" "}
                    <i className="ri-search-line search-icon"></i>
                  </div>
                </Col>
                <Col xl={2} className="ms-auto">
                  <div>
                    <select className="form-control"
                      onChange={(e) => category(e.target.value)}
                    >
                      <option value="All">Select Categories</option>
                      <option value="All">All</option>
                      <option value="Retailer">Retailer</option>
                      <option value="Health & Medicine">
                        Health & Medicine
                      </option>
                      <option value="Manufacturer">Manufacturer</option>
                      <option value="Food Service">Food Service</option>
                      <option value="Computers & Electronics">
                        Computers & Electronics
                      </option>
                    </select>
                  </div>
                </Col>
                <div className="col-lg-auto">
                  <div className="hstack gap-2">
                    <button type="button" className="btn btn-danger">
                      <i className="ri-equalizer-fill me-1 align-bottom"></i>{" "}
                      Filters
                    </button>
                    <button
                      className="btn btn-success"
                      onClick={() => {
                        setModal(true);
                      }}
                    >
                      <i className="ri-add-fill me-1 align-bottom"></i> Add
                      Seller
                    </button>
                  </div>
                </div>
              </Row>
            </CardHeader>
          </Card>

          <Row className="mt-4">
            {sellerList.map((seller, key) => (
              <React.Fragment key={key}>
                <Col xl={3} lg={6}>
                  <Card className="ribbon-box right overflow-hidden">
                    <CardBody className="text-center p-4">
                      {seller.isTrending && (
                        <div className="ribbon ribbon-info ribbon-shape trending-ribbon">
                          <i className="ri-flashlight-fill text-white align-bottom"></i>{" "}
                          <span className="trending-ribbon-text">Trending</span>
                        </div>
                      )}
                      <img src={seller.img} alt="" height="45" />
                      <h5 className="mb-1 mt-4">
                        <Link
                          to="/apps-ecommerce-seller-details"
                          className="link-primary"
                        >
                          {seller.label}
                        </Link>
                      </h5>
                      <p className="text-muted mb-4">{seller.name}</p>
                      <Row className="justify-content-center">
                        <Col lg={8}>
                          <SellerChats
                            color={seller.color}
                            data={seller.chartdata}
                          />
                        </Col>
                      </Row>
                      <Row className="mt-4">
                        <Col lg={6} className="border-end-dashed border-end">
                          <h5>{seller.stock}</h5>
                          <span className="text-muted">Item Stock</span>
                        </Col>
                        <Col lg={6}>
                          <h5>{seller.balance}</h5>
                          <span className="text-muted">Wallet Balance</span>
                        </Col>
                      </Row>
                      <div className="mt-4">
                        <Link
                          to="/apps-ecommerce-seller-details"
                          className="btn btn-light w-100"
                        >
                          View Details
                        </Link>
                      </div>
                    </CardBody>
                  </Card>
                </Col>
              </React.Fragment>
            ))}
          </Row>

          <Row className="g-0 text-center text-sm-start align-items-center mb-3">
            <Col sm={6}>
              <div>
                <p className="mb-sm-0">Showing 1 to 8 of 12 entries</p>
              </div>
            </Col>
            <Col sm={6}>
              <ul className="pagination pagination-separated justify-content-center justify-content-sm-end mb-sm-0">
                <li className="page-item disabled">
                  {" "}
                  <Link to="#" className="page-link">
                    <i className="mdi mdi-chevron-left"></i>
                  </Link>{" "}
                </li>
                <li className="page-item active">
                  {" "}
                  <Link to="#" className="page-link">
                    1
                  </Link>{" "}
                </li>
                <li className="page-item ">
                  {" "}
                  <Link to="#" className="page-link">
                    2
                  </Link>{" "}
                </li>
                <li className="page-item">
                  {" "}
                  <Link to="#" className="page-link">
                    3
                  </Link>{" "}
                </li>
                <li className="page-item">
                  {" "}
                  <Link to="#" className="page-link">
                    4
                  </Link>{" "}
                </li>
                <li className="page-item">
                  {" "}
                  <Link to="#" className="page-link">
                    5
                  </Link>{" "}
                </li>
                <li className="page-item">
                  {" "}
                  <Link to="#" className="page-link">
                    <i className="mdi mdi-chevron-right"></i>
                  </Link>{" "}
                </li>
              </ul>
            </Col>
          </Row>

          <Modal
            className="zoomIn"
            id="addSeller"
            size="lg"
            isOpen={modal}
            toggle={toggle}
            centered
          >
            <ModalHeader toggle={toggle}>Add Seller</ModalHeader>
            <div className="modal-content border-0 mt-3">
              <Nav className="nav-tabs nav-tabs-custom nav-success p-2 pb-0 bg-light">
                <NavItem>
                  <NavLink
                    href="#"
                    className={classnames({ active: activeTab === "1" })}
                    onClick={() => {
                      toggleTab("1");
                    }}
                  >
                    Personal Details
                  </NavLink>
                </NavItem>
                <NavItem>
                  <NavLink
                    href="#"
                    className={classnames({ active: activeTab === "2" })}
                    onClick={() => {
                      toggleTab("2");
                    }}
                  >
                    Business Details
                  </NavLink>
                </NavItem>
                <NavItem>
                  <NavLink
                    href="#"
                    className={classnames({ active: activeTab === "3" })}
                    onClick={() => {
                      toggleTab("3");
                    }}
                  >
                    Bank Details
                  </NavLink>
                </NavItem>
              </Nav>
            </div>
            <ModalBody>
              <TabContent activeTab={activeTab}>
                <TabPane tabId="1">
                  <Form action="#">
                    <Row>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="firstnameInput" className="form-label">
                            First Name
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="firstnameInput"
                            placeholder="Enter your firstname"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="lastnameInput" className="form-label">
                            Last Name
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="lastnameInput"
                            placeholder="Enter your lastname"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label
                            for="contactnumberInput"
                            className="form-label"
                          >
                            Contact Number
                          </Label>
                          <Input
                            type="number"
                            className="form-control"
                            id="contactnumberInput"
                            placeholder="Enter your number"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="phonenumberInput" className="form-label">
                            Phone Number
                          </Label>
                          <Input
                            type="number"
                            className="form-control"
                            id="phonenumberInput"
                            placeholder="Enter your number"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="emailidInput" className="form-label">
                            Email
                          </Label>
                          <Input
                            type="email"
                            className="form-control"
                            id="emailidInput"
                            placeholder="Enter your email"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="birthdayidInput" className="form-label">
                            Date of Birth
                          </Label>
                          <Flatpickr
                            className="form-control"
                            options={{
                              dateFormat: "d M, Y"
                            }}
                            placeholder="Select Date"
                          />
                        </div>
                      </Col>
                      <Col lg={4}>
                        <div className="mb-3">
                          <Label for="cityidInput" className="form-label">
                            City
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="cityidInput"
                            placeholder="Enter your city"
                          />
                        </div>
                      </Col>
                      <Col lg={4}>
                        <div className="mb-3">
                          <Label for="countryidInput" className="form-label">
                            Country
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="countryidInput"
                            placeholder="Enter your country"
                          />
                        </div>
                      </Col>
                      <Col lg={4}>
                        <div className="mb-3">
                          <Label for="zipcodeidInput" className="form-label">
                            Zip Code
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="zipcodeidInput"
                            placeholder="Enter your zipcode"
                          />
                        </div>
                      </Col>
                      <Col lg={12}>
                        <div className="mb-3">
                          <Label
                            for="exampleFormControlTextarea1"
                            className="form-label"
                          >
                            Description
                          </Label>
                          <textarea
                            className="form-control"
                            id="exampleFormControlTextarea1"
                            rows="3"
                            placeholder="Enter description"
                          ></textarea>
                        </div>
                      </Col>
                      <Col lg={12}>
                        <div className="hstack gap-2 justify-content-end">
                          <button
                            className="btn btn-link link-success text-decoration-none fw-medium"
                            data-bs-dismiss="modal"
                          >
                            <i className="ri-close-line me-1 align-middle"></i>{" "}
                            Close
                          </button>
                          <button type="submit" className="btn btn-primary">
                            <i className="ri-save-3-line align-bottom me-1"></i>{" "}
                            Save
                          </button>
                        </div>
                      </Col>
                    </Row>
                  </Form>
                </TabPane>
                <TabPane tabId="2">
                  <form action="#">
                    <Row>
                      <Col lg={12}>
                        <div className="mb-3">
                          <Label for="companynameInput" className="form-label">
                            Company Name
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="companynameInput"
                            placeholder="Enter your company name"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label
                            for="choices-single-default"
                            className="form-label"
                          >
                            Company Type
                          </Label>
                          <Select
                            name="choices-single-default"
                            id="choices-single-default"
                            value={companyType}
                            onChange={() => {
                              handlecompanyType();
                            }}
                            options={companytypes}
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="pancardInput" className="form-label">
                            Pan Card Number
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="pancardInput"
                            placeholder="Enter your pan-card number"
                          />
                        </div>
                      </Col>
                      <Col lg={4}>
                        <div className="mb-3">
                          <Label for="websiteInput" className="form-label">
                            Website
                          </Label>
                          <Input
                            type="url"
                            className="form-control"
                            id="websiteInput"
                            placeholder="Enter your URL"
                          />
                        </div>
                      </Col>
                      <Col lg={4}>
                        <div className="mb-3">
                          <Label for="faxInput" className="form-label">
                            Fax
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="faxInput"
                            placeholder="Enter your fax"
                          />
                        </div>
                      </Col>
                      <Col lg={4}>
                        <div className="mb-3">
                          <Label for="companyemailInput" className="form-label">
                            Email
                          </Label>
                          <Input
                            type="email"
                            className="form-control"
                            id="companyemailInput"
                            placeholder="Enter your email"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="worknumberInput" className="form-label">
                            Number
                          </Label>
                          <Input
                            type="number"
                            className="form-control"
                            id="worknumberInput"
                            placeholder="Enter your number"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="companylogoInput" className="form-label">
                            Company Logo
                          </Label>
                          <Input
                            type="file"
                            className="form-control"
                            id="companylogoInput"
                          />
                        </div>
                      </Col>
                      <Col lg={12}>
                        <div className="hstack gap-2 justify-content-end">
                          <button
                            className="btn btn-link link-success text-decoration-none fw-medium"
                            data-bs-dismiss="modal"
                          >
                            <i className="ri-close-line me-1 align-middle"></i>{" "}
                            Close
                          </button>
                          <button type="submit" className="btn btn-primary">
                            <i className="ri-save-3-line align-bottom me-1"></i>{" "}
                            Save
                          </button>
                        </div>
                      </Col>
                    </Row>
                  </form>
                </TabPane>
                <TabPane tabId="3">
                  <form action="#">
                    <div className="row">
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="banknameInput" className="form-label">
                            Bank Name
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="banknameInput"
                            placeholder="Enter your bank name"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="branchInput" className="form-label">
                            Branch
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="branchInput"
                            placeholder="Branch"
                          />
                        </div>
                      </Col>
                      <Col lg={12}>
                        <div className="mb-3">
                          <Label for="accountnameInput" className="form-label">
                            Account Holder Name
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="accountnameInput"
                            placeholder="Enter account holder name"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label
                            for="accountnumberInput"
                            className="form-label"
                          >
                            Account Number
                          </Label>
                          <Input
                            type="number"
                            className="form-control"
                            id="accountnumberInput"
                            placeholder="Enter account number"
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div className="mb-3">
                          <Label for="ifscInput" className="form-label">
                            IFSC
                          </Label>
                          <Input
                            type="number"
                            className="form-control"
                            id="ifscInput"
                            placeholder="IFSC"
                          />
                        </div>
                      </Col>
                      <Col lg={12}>
                        <div className="hstack gap-2 justify-content-end">
                          <button
                            className="btn btn-link link-success text-decoration-none fw-medium"
                            data-bs-dismiss="modal"
                          >
                            <i className="ri-close-line me-1 align-middle"></i>{" "}
                            Close
                          </button>
                          <button type="submit" className="btn btn-primary">
                            <i className="ri-save-3-line align-bottom me-1"></i>{" "}
                            Save
                          </button>
                        </div>
                      </Col>
                    </div>
                  </form>
                </TabPane>
              </TabContent>
            </ModalBody>
          </Modal>
        </Container>
      </div>
    </React.Fragment>
  );
};

export default EcommerceSellers;