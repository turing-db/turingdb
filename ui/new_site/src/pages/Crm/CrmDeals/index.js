import React, { useEffect, useState } from "react";

import {
  Col,
  Container,
  Row,
  Card,
  CardBody,
  Input,
  Button,
  UncontrolledDropdown,
  DropdownToggle,
  DropdownMenu,
  DropdownItem,
  Modal,
  Form,
  ModalBody,
  ModalFooter,
  ModalHeader,
} from "reactstrap";

import BreadCrumb from "../../../Components/Common/BreadCrumb";
import Select from "react-select";
import LeadDiscover from "./leadDiscover";


//Import actions
import { getDeals as onGetDeals } from "../../../slices/thunks";
//redux
import { useSelector, useDispatch } from "react-redux";

const CrmDeals = () => {
  document.title="Deals | Velzon - React Admin & Dashboard Template";

  const dispatch = useDispatch();
  const { deals } = useSelector((state) => ({
    deals: state.Crm.deals,
  }));

  useEffect(() => {
    if (deals && !deals.length) {
      dispatch(onGetDeals());
    }
  }, [dispatch, deals]);

  const [sortBy, setsortBy] = useState("Owner");
  const [modal, setModal] = useState(false);

  function handlesortBy(sortBy) {
    setsortBy(sortBy);
  }

  const sortbyname = [
    {
      options: [
        { label: "Owner", value: "Owner" },
        { label: "Company", value: "Company" },
        { label: "Location", value: "Location" },
      ],
    },
  ];

  const toggle = () => {
    if (modal) {
      setModal(false);
    } else {
      setModal(true);
    }
  };

  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="Deals" pageTitle="CRM" />
          <Card>
            <CardBody>
              <Row className="g-3">
                <Col md={3}>
                  <div className="search-box">
                    <Input
                      type="text"
                      className="form-control search"
                      placeholder="Search for deals..."
                    />
                    <i className="ri-search-line search-icon"></i>
                  </div>
                </Col>
                <div className="col-md-auto ms-auto">
                  <div className="d-flex hastck gap-2 flex-wrap">
                    <div className="d-flex align-items-center gap-2">
                      <span className="text-muted">Sort by: </span>
                      <Select
                        className="mb-0"
                        value={sortBy}
                        onChange={() => {
                          handlesortBy();
                        }}
                        options={sortbyname}
                        id="choices-single-default"
                      ></Select>
                    </div>
                    <button className="btn btn-success" onClick={toggle}>
                      <i className="ri-add-fill align-bottom me-1"></i> Add
                      Deals
                    </button>
                    <UncontrolledDropdown>
                      <DropdownToggle
                        href="#"
                        className="btn btn-soft-info btn-icon fs-14"
                        tag="button"
                      >
                        <i className="ri-settings-4-line"></i>
                      </DropdownToggle>
                      <DropdownMenu className="dropdown-menu-end">
                        <DropdownItem className="dropdown-item" href="#">
                          Copy
                        </DropdownItem>
                        <DropdownItem className="dropdown-item" href="#">
                          Move to pipline
                        </DropdownItem>
                        <DropdownItem className="dropdown-item" href="#">
                          Add to exceptions
                        </DropdownItem>
                        <DropdownItem className="dropdown-item" href="#">
                          Switch to common form view
                        </DropdownItem>
                        <DropdownItem className="dropdown-item" href="#">
                          Reset form view to default
                        </DropdownItem>
                      </DropdownMenu>
                    </UncontrolledDropdown>
                  </div>
                </div>
              </Row>
            </CardBody>
          </Card>

          <Row className="row-cols-xxl-5 row-cols-lg-3 row-cols-md-2 row-cols-1">
            {deals.map((deal, key) => (
              <React.Fragment key={key}>
                <LeadDiscover deal={deal} index={key} />
              </React.Fragment>
            ))}
          </Row>
        </Container>
      </div>

      <Modal id="adddeals" isOpen={modal} toggle={toggle} centered>
        <ModalHeader className="bg-light p-3" toggle={toggle}>
          Create Deals
        </ModalHeader>
        <Form>
          <ModalBody>
            <div className="mb-3">
              <label htmlFor="deatType" className="form-label">Deals Type</label>
              <select className="form-select" id="deatType" data-choices
                aria-label="Default select example" required>
                <option value=""
                >Select deals type</option>
                <option value="Lead Disovered">Lead Disovered</option>
                <option value="Contact Initiated">Contact Initiated</option>
                <option value="Need Identified">Need Identified</option>
                <option value="Meeting Arranged">Meeting Arranged</option>
                <option value="Offer Accepted">Offer Accepted</option>
              </select>
              <div className="invalid-feedback">
                Please write an deals owner name.
              </div>
              
            </div>

            <div className="mb-3">
              <label htmlFor="dealTitle" className="form-label">Deal Title</label>
              <input type="text" className="form-control" id="dealTitle"
                placeholder="Enter title" required />
              <div className="invalid-feedback">
                Please write a title.
              </div>
            </div>

            <div className="mb-3">
              <label htmlFor="dealValue" className="form-label">Value (USD)</label>
              <input type="number" className="form-control" id="dealValue" step="0.01"
                placeholder="Enter value" required />
              <div className="invalid-feedback">
                Please write a value.
              </div>
            </div>

            <div className="mb-3">
              <label htmlFor="dealOwner" className="form-label">Deals Owner</label>
              <input type="text" className="form-control" id="dealOwner" required
                placeholder="Enter owner name" />
              <div className="invalid-feedback">
                Please write an deals owner name.
              </div>
            </div>
            <div className="mb-3">
              <label htmlFor="dueDate" className="form-label">Due Date</label>
              <input type="text" className="form-control" id="dueDate"
                data-provider="flatpickr" placeholder="Select date" required />
              <div className="invalid-feedback">
                Please select a due date.
              </div>
            </div>

            <div className="mb-3">
              <label htmlFor="dealEmail" className="form-label">Email</label>
              <input type="email" className="form-control" id="dealEmail"
                placeholder="Enter email" required />
              <div className="invalid-feedback">
                Please write a email.
              </div>
            </div>

            <div className="mb-3">
              <label htmlFor="contactNumber" className="form-label">Contact</label>
              <input type="text" className="form-control" id="contactNumber"
                placeholder="Enter contact number" required />
              <div className="invalid-feedback">
                Please add a contact.
              </div>
            </div>
            <div className="mb-3">
              <label htmlFor="contactDescription" className="form-label">Description</label>
              <textarea className="form-control" id="contactDescription" rows="3"
                placeholder="Enter description" required></textarea>
              <div className="invalid-feedback">
                Please add a description.
              </div>
            </div>
          </ModalBody>
          <ModalFooter>
            <Button
              type="button"
              color="light"
              id="close-modal"
              onClick={() => {
                setModal(false);
              }}
            >
              Close
            </Button>
            <Button
              type="submit"
              color="success"
              onClick={() => {
                setModal(false);
              }}
            >
              <i className="ri-save-line align-bottom me-1"></i> Save
            </Button>
          </ModalFooter>
        </Form>
      </Modal>
    </React.Fragment>
  );
};

export default CrmDeals;
