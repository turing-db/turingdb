import React from "react";
import { Col, Container, Row } from "reactstrap";
import BreadCrumb from "../../../../Components/Common/BreadCrumb";
import Header from "./Header";
import JobDescription from "./JobDescription";
import RelatedJobs from "./RelatedJobs";
import RightSection from "./RightSection";

const JobOverview = () => {
  document.title = "Job Overview | Velzon -  Admin & Dashboard Template";

  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          {/* <BreadCrumb title="Job Overview" pageTitle="Jobs" /> */}

          <Row className="row">
            <Header />
          </Row>

          <Row className="row mt-n5">
            <Col className="col-xxl-9">
              <JobDescription />

              <RelatedJobs />
            </Col>
            <Col className="col-xxl-3">
              <RightSection />
            </Col>
          </Row>
        </Container>
      </div>
    </React.Fragment>
  );
};

export default JobOverview;
