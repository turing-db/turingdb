import React from "react";
import { NavLink } from "react-router-dom";
import { Card, CardBody, Col, Container, Row } from "reactstrap";
import BreadCrumb from "../../../Components/Common/BreadCrumb";

const TermsCondition = () => {
  document.title =
    "Terms & Condition | Velzon - React Admin & Dashboard Template";
  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="Terms & Condition" pageTitle="Pages" />
          <Row className="justify-content-center">
            <Col className="col-lg-10">
              <Card>
                <div className="bg-soft-warning position-relative">
                  <CardBody className="card-body p-5">
                    <div className="text-center">
                      <h3>Term & Conditions</h3>
                      <p className="mb-0 text-muted">
                        Last update: 16 Sept, 2022
                      </p>
                    </div>
                  </CardBody>
                  <div className="shape">
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      version="1.1"
                      xmlnsXlink="http://www.w3.org/1999/xlink"
                      // xmlnsSvgjs="http://svgjs.com/svgjs"
                      width="1440"
                      height="60"
                      preserveAspectRatio="none"
                      viewBox="0 0 1440 60"
                    >
                      <g mask='url("#SvgjsMask1001")' fill="none">
                        <path
                          d="M 0,4 C 144,13 432,48 720,49 C 1008,50 1296,17 1440,9L1440 60L0 60z"
                          style={{ fill: "var(--vz-card-bg-custom)" }}
                        ></path>
                      </g>
                      {/* <defs>
                        <mask id="SvgjsMask1001">
                          <rect width="1440" height="60" fill="#ffffff"></rect>
                        </mask>
                      </defs> */}
                    </svg>
                  </div>
                </div>
                <CardBody className="card-body p-4">
                  <div>
                    <h5>Welcome to Velzon!</h5>
                    <p className="text-muted">
                      These terms and conditions outline the rules and
                      regulations for the use of Company Name's Website, located
                      at Website.com.
                    </p>
                    <p className="text-muted">
                      By accessing this website we assume you accept these terms
                      and conditions. Do not continue to use Website Name if you
                      do not agree to take all of the terms and conditions
                      stated on this page.
                    </p>
                    <p className="text-muted">
                      Most interactive websites use cookies to let us retrieve
                      the user's details for each visit. Cookies are used by our
                      website to enable the functionality of certain areas to
                      make it easier for people visiting our website. Some of
                      our affiliate/advertising partners may also use cookies.
                    </p>
                  </div>

                  <div>
                    <h5>License</h5>
                    <p className="text-muted">
                      Unless otherwise stated, Company Name and/or its licensors
                      own the intellectual property rights for all material on
                      Website Name. All intellectual property rights are
                      reserved. You may access this from Website Name for your
                      own personal use subjected to restrictions set in these
                      terms and conditions.
                    </p>
                    <p className="text-muted">You must not:</p>
                    <ul className="text-muted vstack gap-2">
                      <li>Republish material from Website Name</li>
                      <li>
                        Sell, rent or sub-license material from Website Name
                      </li>
                      <li>
                        Reproduce, duplicate or copy material from Website Name
                      </li>
                      <li>Redistribute content from Website Name</li>
                    </ul>
                    <p className="text-muted">
                      This Agreement shall begin on the date hereof.
                    </p>
                    <p className="text-muted">
                      Parts of this website offer an opportunity for users to
                      post and exchange opinions and information in certain
                      areas of the website. Company Name does not filter, edit,
                      publish or review Comments prior to their presence on the
                      website. Comments do not reflect the views and opinions of
                      Company Name,its agents and/or affiliates.
                    </p>
                  </div>

                  <div>
                    <p className="text-muted">
                      Company Name reserves the right to monitor all Comments
                      and to remove any Comments which can be considered
                      inappropriate, offensive or causes breach of these Terms
                      and Conditions.
                    </p>
                    <p className="text-muted">
                      You warrant and represent that:
                    </p>
                    <ul className="text-muted vstack gap-2">
                      <li>
                        You are entitled to post the Comments on our website and
                        have all necessary licenses and consents to do so;
                      </li>
                      <li>
                        The Comments do not invade any intellectual property
                        right, including without limitation copyright, patent or
                        trademark of any third party;
                      </li>
                      <li>
                        The Comments do not contain any defamatory, libelous,
                        offensive, indecent or otherwise unlawful material which
                        is an invasion of privacy
                      </li>
                      <li>
                        The Comments will not be used to solicit or promote
                        business or custom or present commercial activities or
                        unlawful activity.
                      </li>
                    </ul>
                    <p className="text-muted">
                      You hereby grant Company Name a non-exclusive license to
                      use, reproduce, edit and authorize others to use,
                      reproduce and edit any of your Comments in any and all
                      forms, formats or media.
                    </p>
                    <p className="text-muted">
                      Approved organizations may hyperlink to our Website as
                      follows:
                    </p>
                    <ul className="text-muted vstack gap-2">
                      <li>By use of our corporate name; or</li>
                      <li>
                        By use of the uniform resource locator being linked to;
                        or
                      </li>
                      <li>
                        By use of any other description of our Website being
                        linked to that makes sense within the context and format
                        of content on the linking party's site.
                      </li>
                    </ul>
                    <p className="text-muted fw-semibold">
                      No use of Company Name's logo or other artwork will be
                      allowed for linking absent a trademark license agreement.
                    </p>
                  </div>

                  <div className="text-end">
                    <NavLink to="#!" className="btn btn-success">
                      Accept
                    </NavLink>
                    <NavLink to="#!" className="btn btn-outline-danger">
                      <i className="ri-close-line align-bottom me-1"></i>{" "}
                      Decline
                    </NavLink>
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

export default TermsCondition;
