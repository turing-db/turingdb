import React from 'react'
import { Container, Row } from 'reactstrap';
import BreadCrumb from '../../Components/Common/BreadCrumb';
import ApplicationsStatistic from './ApplicationsStatistic';
import Candidates from './Candidates';
import FeaturedCompanies from './FeaturedCompanies';
import RecomendedJobs from './RecomendedJobs';
import RecentApplicants from './RecentApplicants';
import SalesByLocations from './SalesByLocations';
import Widgets from './Widgets';

const index = () => {
  document.title = "Jobs Dashboard | Velzon - React Admin & Dashboard Template";

  return (
    <React.Fragment>
      <div className="page-content">
        <Container fluid>
          <BreadCrumb title="Job Dashboard" pageTitle="Dashboards" />
          <Row>
            <Widgets />
            <FeaturedCompanies />
          </Row>

          <Row>
            <ApplicationsStatistic dataColors='["--vz-primary", "--vz-secondary", "--vz-success"]' />
            <Candidates />
          </Row>

          <Row>
            <RecomendedJobs />
          </Row>

          <Row>
            <RecentApplicants />
            <SalesByLocations />
          </Row>
        </Container>
      </div>
    </React.Fragment>
  )
}

export default index