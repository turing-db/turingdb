import React, { useState } from "react";
import {
  Offcanvas,
  OffcanvasHeader,
  OffcanvasBody,
  Label,
  Input,
  Row,
  Col,
} from "reactstrap";
import Flatpickr from "react-flatpickr";
import Select from "react-select";

const CrmFilter = ({ show, onCloseClick }) => {
  const [selectCountry, setselectCountry] = useState(null);

  function handleselectCountry(selectCountry) {
    setselectCountry(selectCountry);
  }

  const country = [
    {
      options: [
        { label: "Select country", value: "Select country" },
        { label: "Argentina", value: "Argentina" },
        { label: "Belgium", value: "Belgium" },
        { label: "Brazil", value: "Brazil" },
        { label: "Colombia", value: "Colombia" },
        { label: "Denmark", value: "Denmark" },
        { label: "France", value: "France" },
        { label: "Germany", value: "Germany" },
        { label: "Mexico", value: "Mexico" },
        { label: "Russia", value: "Russia" },
        { label: "Spain", value: "Spain" },
        { label: "Syria", value: "Syria" },
        { label: "United Kingdom", value: "United Kingdom" },
        {
          label: "United States of America",
          value: "United States of America",
        },
      ],
    },
  ];

  return (
    <Offcanvas
      direction="end"
      isOpen={show}
      id="offcanvasExample"
      toggle={onCloseClick}
    >
      <OffcanvasHeader className="bg-light" toggle={onCloseClick}>
        Leads Fliters
      </OffcanvasHeader>
      <form action="" className="d-flex flex-column justify-content-end h-100">
        <OffcanvasBody>
          <div className="mb-4">
            <Label
              htmlFor="datepicker-range"
              className="form-label text-muted text-uppercase fw-semibold mb-3"
            >
              Date
            </Label>
            <Flatpickr
              className="form-control"
              id="datepicker-publish-input"
              placeholder="Select a date"
              options={{
                altInput: true,
                altFormat: "F j, Y",
                mode: "multiple",
                dateFormat: "d.m.y",
              }}
            />
          </div>
          <div className="mb-4">
            <Label
              htmlFor="country-select"
              className="form-label text-muted text-uppercase fw-semibold mb-3"
            >
              Country
            </Label>

            <Select
              className="mb-0"
              value={selectCountry}
              onChange={() => {
                handleselectCountry();
              }}
              options={country}
              id="country-select"
            ></Select>
          </div>
          <div className="mb-4">
            <Label
              htmlFor="status-select"
              className="form-label text-muted text-uppercase fw-semibold mb-3"
            >
              Status
            </Label>
            <Row className="g-2">
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="inlineCheckbox1"
                    defaultValue="option1"
                  />
                  <Label className="form-check-label" htmlFor="inlineCheckbox1">
                    New Leads
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="inlineCheckbox2"
                    defaultValue="option2"
                  />
                  <Label className="form-check-label" htmlFor="inlineCheckbox2">
                    Old Leads
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="inlineCheckbox3"
                    defaultValue="option3"
                  />
                  <Label className="form-check-label" htmlFor="inlineCheckbox3">
                    Loss Leads
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="inlineCheckbox4"
                    defaultValue="option4"
                  />
                  <Label className="form-check-label" htmlFor="inlineCheckbox4">
                    Follow Up
                  </Label>
                </div>
              </Col>
            </Row>
          </div>
          <div className="mb-4">
            <Label
              htmlFor="leadscore"
              className="form-label text-muted text-uppercase fw-semibold mb-3"
            >
              Lead Score
            </Label>
            <Row className="g-2 align-items-center">
              <div className="col-lg">
                <Input
                  type="number"
                  className="form-control"
                  id="leadscore"
                  placeholder="0"
                />
              </div>
              <div className="col-lg-auto">To</div>
              <div className="col-lg">
                <Input
                  type="number"
                  className="form-control"
                  id="leadscore"
                  placeholder="0"
                />
              </div>
            </Row>
          </div>
          <div>
            <Label
              htmlFor="leads-tags"
              className="form-label text-muted text-uppercase fw-semibold mb-3"
            >
              Tags
            </Label>
            <Row className="g-3">
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="marketing"
                    defaultValue="marketing"
                  />
                  <Label className="form-check-label" htmlFor="marketing">
                    Marketing
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="management"
                    defaultValue="management"
                  />
                  <Label className="form-check-label" htmlFor="management">
                    Management
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="business"
                    defaultValue="business"
                  />
                  <Label className="form-check-label" htmlFor="business">
                    Business
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="investing"
                    defaultValue="investing"
                  />
                  <Label className="form-check-label" htmlFor="investing">
                    Investing
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="partner"
                    defaultValue="partner"
                  />
                  <Label className="form-check-label" htmlFor="partner">
                    Partner
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="lead"
                    defaultValue="lead"
                  />
                  <Label className="form-check-label" htmlFor="lead">
                    Leads
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="sale"
                    defaultValue="sale"
                  />
                  <Label className="form-check-label" htmlFor="sale">
                    Sale
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="owner"
                    defaultValue="owner"
                  />
                  <Label className="form-check-label" htmlFor="owner">
                    Owner
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="banking"
                    defaultValue="banking"
                  />
                  <Label className="form-check-label" htmlFor="banking">
                    Banking
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="banking"
                    defaultValue="banking"
                  />
                  <Label className="form-check-label" htmlFor="banking">
                    Exiting
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="banking"
                    defaultValue="banking"
                  />
                  <Label className="form-check-label" htmlFor="banking">
                    Finance
                  </Label>
                </div>
              </Col>
              <Col lg={6}>
                <div className="form-check">
                  <Input
                    className="form-check-input"
                    type="checkbox"
                    id="banking"
                    defaultValue="banking"
                  />
                  <Label className="form-check-label" htmlFor="banking">
                    Fashion
                  </Label>
                </div>
              </Col>
            </Row>
          </div>
        </OffcanvasBody>
        <div className="offcanvas-footer border-top p-3 text-center hstack gap-2">
          <button
            className="btn btn-light w-100"
            onClick={onCloseClick}
          >
            Clear Filter
          </button>
          <button
            type="submit"
            className="btn btn-success w-100"
            onClick={onCloseClick}
          >
            Filters
          </button>
        </div>
      </form>
    </Offcanvas>
  );
};

export default CrmFilter;
