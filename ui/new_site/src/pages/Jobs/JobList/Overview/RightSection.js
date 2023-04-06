import React from "react";
import { Link, NavLink } from "react-router-dom";
import { Card, CardBody, CardHeader, Table } from "reactstrap";
import img3 from "../../../../assets/images/companies/img-3.png";
const RightSection = () => {
  return (
    <React.Fragment>
      <Card>
        <CardHeader>
          <h5 className="mb-0">Job Overview</h5>
        </CardHeader>
        <CardBody>
          <div className="table-responsive table-card">
            <Table className="table mb-0">
              <tbody>
                <tr>
                  <td className="fw-medium">Title</td>
                  <td>Product Designer</td>
                </tr>
                <tr>
                  <td className="fw-medium">Company Name</td>
                  <td>Themesbrand</td>
                </tr>
                <tr>
                  <td className="fw-medium">Location</td>
                  <td>Zuweihir, UAE</td>
                </tr>
                <tr>
                  <td className="fw-medium">Time</td>
                  <td>
                    <span className="badge badge-soft-success">Full Time</span>
                  </td>
                </tr>
                <tr>
                  <td className="fw-medium">Job Application</td>
                  <td>54 Application</td>
                </tr>
                <tr>
                  <td className="fw-medium">Post Date</td>
                  <td>15 Sep, 2021</td>
                </tr>
                <tr>
                  <td className="fw-medium">Salary</td>
                  <td>$35k - $45k</td>
                </tr>
                <tr>
                  <td className="fw-medium">Experience</td>
                  <td>5+ Years</td>
                </tr>
                <tr>
                  <td className="fw-medium">Qualification</td>
                  <td>Master Degree</td>
                </tr>
              </tbody>
            </Table>
          </div>
          <div className="mt-4 pt-2 hstack gap-2">
            <Link to="#!" className="btn btn-primary w-100">
              Apply Now
            </Link>
            <Link
              to="#!"
              className="btn btn-soft-danger btn-icon custom-toggle flex-shrink-0"
              data-bs-toggle="button"
            >
              <span className="icon-on">
                <i className="ri-bookmark-line align-bottom"></i>
              </span>
              <span className="icon-off">
                <i className="ri-bookmark-3-fill align-bottom"></i>
              </span>
            </Link>
          </div>
        </CardBody>
      </Card>

      <Card>
        <CardBody>
          <div className="avatar-sm mx-auto">
            <div className="avatar-title bg-soft-warning rounded">
              <img src={img3} alt="" className="avatar-xxs" />
            </div>
          </div>
          <div className="text-center">
            <Link to="#!">
              <h5 className="mt-3">Themesbrand</h5>
            </Link>
            <p className="text-muted">IT Department</p>
          </div>

          <div className="table-responsive">
            <Table className="mb-0">
              <tbody>
                <tr>
                  <td className="fw-semibold">Company Size</td>
                  <td>5</td>
                </tr>
                <tr>
                  <td className="fw-semibold">Industry Type</td>
                  <td>Software</td>
                </tr>
                <tr>
                  <td className="fw-semibold">Founded in</td>
                  <td>2016</td>
                </tr>
                <tr>
                  <td className="fw-semibold">Phone</td>
                  <td>+(234) 12345 67890</td>
                </tr>
                <tr>
                  <td className="fw-semibold">Email</td>
                  <td>themesbrand@gmail.com</td>
                </tr>
                <tr>
                  <td className="fw-semibold">Social media</td>
                  <td>
                    <ul className="list-inline mb-0">
                      <li className="list-inline-item">
                        <Link to="#!">
                          <i className="ri-whatsapp-line"></i>
                        </Link>
                      </li>
                      <li className="list-inline-item">
                        <Link to="#!">
                          <i className="ri-facebook-line"></i>
                        </Link>
                      </li>
                      <li className="list-inline-item">
                        <Link to="#!">
                          <i className="ri-twitter-line"></i>
                        </Link>
                      </li>
                      <li className="list-inline-item">
                        <Link to="#!">
                          <i className="ri-youtube-line"></i>
                        </Link>
                      </li>
                    </ul>
                  </td>
                </tr>
              </tbody>
            </Table>
          </div>
        </CardBody>
      </Card>

      <Card>
        <CardHeader>
          <h5 className="mb-0">Job Location</h5>
        </CardHeader>
        <CardBody>
          <div className="ratio ratio-4x3">
            <iframe
              src="https://www.google.com/maps/embed?pb=!1m18!1m12!1m3!1d1861227.8137337372!2d51.654904288504646!3d24.33915646928631!2m3!1f0!2f0!3f0!3m2!1i1024!2i768!4f13.1!3m3!1m2!1s0x3e5e48dfb1ab12bd%3A0x33d32f56c0080aa7!2sUnited%20Arab%20Emirates!5e0!3m2!1sen!2sin!4v1664257145153!5m2!1sen!2sin"
              height="450"
              style={{ border: "0" }}
              title="abc"
              allowFullScreen=""
              loading="lazy"
              referrerPolicy="no-referrer-when-downgrade"
            ></iframe>
          </div>
        </CardBody>
      </Card>

      <Card>
        <CardHeader>
          <h5 className="mb-0">Contact Us</h5>
        </CardHeader>
        <CardBody>
          <form>
            <div className="mb-3">
              <label htmlFor="nameInput" className="form-label">
                Name
              </label>
              <input
                type="text"
                className="form-control"
                id="nameInput"
                placeholder="Enter your name"
              />
            </div>
            <div className="mb-3">
              <label htmlFor="emailInput" className="form-label">
                Email
              </label>
              <input
                type="text"
                className="form-control"
                id="emailInput"
                placeholder="Enter your email"
              />
            </div>
            <div className="mb-3">
              <label htmlFor="messageInput" className="form-label">
                Message
              </label>
              <textarea
                className="form-control"
                id="messageInput"
                rows="3"
                placeholder="Message"
              ></textarea>
            </div>
            <div className="text-end">
              <button type="submit" className="btn btn-primary">
                Send Message
              </button>
            </div>
          </form>
        </CardBody>
      </Card>
    </React.Fragment>
  );
};

export default RightSection;
