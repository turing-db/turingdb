import React from "react";
import {
  Card,
  CardBody,
  CardHeader,
  Col,
  Container,
  Form,
  Input,
  Label,
  Row,
} from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";
import Flatpickr from "react-flatpickr";

const NewJobs = () => {
  document.title = "New Job | Velzon -  Admin & Dashboard Template";

  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="New Job" pageTitle="Jobs" />

          <Row>
            <Col lg={12}>
              <Card>
                <Form action="#">
                  <CardHeader>
                    <h5 className="card-title mb-0">Create Job</h5>
                  </CardHeader>
                  <CardBody>
                    <Row className="row g-4">
                      <Col lg={6}>
                        <div>
                          <Label
                            htmlFor="job-title-Input"
                            className="form-label"
                          >
                            Job Title <span className="text-danger">*</span>
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="job-title-Input"
                            placeholder="Enter job title"
                            required
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div>
                          <Label
                            htmlFor="job-position-Input"
                            className="form-label"
                          >
                            Job Position <span className="text-danger">*</span>
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="job-position-Input"
                            placeholder="Enter job position"
                            required
                          />
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div>
                          <Label
                            htmlFor="job-category-Input"
                            className="form-label"
                          >
                            Job Category <span className="text-danger">*</span>
                          </Label>
                          <select
                            className="form-control"
                            data-choices
                            name="job-category-Input"
                            required
                          >
                            <option value="">Select Category</option>
                            <option value="Accounting & Finance">
                              Accounting & Finance
                            </option>
                            <option value="Purchasing Manager">
                              Purchasing Manager
                            </option>
                            <option value="Education & training">
                              Education & training
                            </option>
                            <option value="Marketing & Advertising">
                              Marketing & Advertising
                            </option>
                            <option value="It / Software Jobs">
                              It / Software Jobs
                            </option>
                            <option value="Digital Marketing">
                              Digital Marketing
                            </option>
                            <option value="Administrative Officer">
                              Administrative Officer
                            </option>
                            <option value="Government Jobs">
                              Government Jobs
                            </option>
                          </select>
                        </div>
                      </Col>
                      <Col lg={6}>
                        <div>
                          <Label
                            htmlFor="job-type-Input"
                            className="form-label"
                          >
                            Job Type <span className="text-danger">*</span>
                          </Label>
                          <select
                            className="form-control"
                            data-choices
                            name="job-type-Input"
                            required
                          >
                            <option value="">Select job type</option>
                            <option value="Full Time">Full Time</option>
                            <option value="Part Time">Part Time</option>
                            <option value="Freelance">Freelance</option>
                            <option value="Intership">Intership</option>
                          </select>
                        </div>
                      </Col>

                      <Col lg={12}>
                        <div>
                          <Label
                            htmlFor="description-field"
                            className="form-label"
                          >
                            Description <span className="text-danger">*</span>
                          </Label>
                          <textarea
                            className="form-control"
                            id="description-field"
                            rows="3"
                            placeholder="Enter description"
                            required
                          ></textarea>
                        </div>
                      </Col>

                      <Col md={6}>
                        <div>
                          <Label
                            htmlFor="vancancy-Input"
                            className="form-label"
                          >
                            No. of Vancancy{" "}
                            <span className="text-danger">*</span>
                          </Label>
                          <Input
                            type="number"
                            className="form-control"
                            id="vancancy-Input"
                            placeholder="No. of vancancy"
                            required
                          />
                        </div>
                      </Col>
                      <Col md={6}>
                        <div>
                          <Label
                            htmlFor="experience-Input"
                            className="form-label"
                          >
                            Experience <span className="text-danger">*</span>
                          </Label>
                          <select
                            className="form-control"
                            data-choices
                            name="experience-Input"
                          >
                            <option value="">Select Experience</option>
                            <option value="0 Year">0 Year</option>
                            <option value="2 Yeasr">2 Years</option>
                            <option value="3 Years">3 Years</option>
                            <option value="4 Years">4 Years</option>
                            <option value="5 Years">5 Years</option>
                          </select>
                        </div>
                      </Col>

                      <Col lg={6}>
                        <div>
                          <Label
                            htmlFor="last-apply-date-Input"
                            className="form-label"
                          >
                            Last Date of Apply{" "}
                            <span className="text-danger">*</span>
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
                      </Col>

                      <Col lg={6}>
                        <div>
                          <Label
                            htmlFor="close-date-Input"
                            className="form-label"
                          >
                            Close Date <span className="text-danger">*</span>
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
                      </Col>

                      <Col md={6}>
                        <div>
                          <Label
                            htmlFor="start-salary-Input"
                            className="form-label"
                          >
                            Start Salary
                          </Label>
                          <Input
                            type="number"
                            className="form-control"
                            id="start-salary-Input"
                            placeholder="Enter start salary"
                            required
                          />
                        </div>
                      </Col>

                      <Col md={6}>
                        <div>
                          <Label
                            htmlFor="last-salary-Input"
                            className="form-label"
                          >
                            Last Salary
                          </Label>
                          <Input
                            type="number"
                            className="form-control"
                            id="last-salary-Input"
                            placeholder="Enter end salary"
                            required
                          />
                        </div>
                      </Col>

                      <Col md={6}>
                        <div>
                          <Label htmlFor="country-Input" className="form-label">
                            Country <span className="text-danger">*</span>
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="country-Input"
                            placeholder="Enter country"
                            required
                          />
                        </div>
                      </Col>

                      <Col md={6}>
                        <div>
                          <Label htmlFor="city-Input" className="form-label">
                            State <span className="text-danger">*</span>
                          </Label>
                          <Input
                            type="text"
                            className="form-control"
                            id="city-Input"
                            placeholder="Enter city"
                            required
                          />
                        </div>
                      </Col>

                      <Col lg={12}>
                        <div>
                          <Label htmlFor="website-field" className="form-label">
                            Tags
                          </Label>
                          <Input
                            className="form-control"
                            id="choices-text-unique-values"
                            data-choices
                            data-choices-text-unique-true
                            type="text"
                            defaultValue="Design, Remote"
                            required
                          />
                        </div>
                      </Col>

                      <Col lg={12}>
                        <div className="hstack justify-content-end gap-2">
                          <button
                            type="button"
                            className="btn btn-ghost-danger"
                          >
                            <i className="ri-close-line align-bottom"></i>{" "}
                            Cancel
                          </button>
                          <button type="submit" className="btn btn-secondary">
                            Add Job
                          </button>
                        </div>
                      </Col>
                    </Row>
                  </CardBody>
                </Form>
              </Card>
            </Col>
          </Row>
        </Container>
      </div>
    </React.Fragment>
  );
};

export default NewJobs;
