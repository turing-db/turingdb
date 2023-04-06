import React from 'react';
import { Card, CardBody, CardHeader, Col, Container, Row } from 'reactstrap';
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import { Link } from 'react-router-dom';

const SiteMap = () => {
    document.title = "SiteMap | Velzon - React Admin & Dashboard Template";

    return (
        <div>
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="SiteMap" pageTitle="Pages" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Responsive Sitemap</h4>
                                </CardHeader>
                                <CardBody>
                                    <div className="sitemap-content">
                                        <figure className="sitemap-horizontal">
                                            <ul className="administration">
                                                <li>
                                                    <ul className="director">
                                                        <li>
                                                            <Link to="#"
                                                                className="fw-semibold"><span>Velzon Admin</span></Link>
                                                            <ul className="subdirector">
                                                                <li><Link to="#" className="fw-semibold"><span>Contact Us</span></Link>
                                                                </li>
                                                            </ul>
                                                            <ul className="departments">
                                                                <li><Link to="#"
                                                                    className="fw-semibold"><span>Main Pages</span></Link>
                                                                </li>

                                                                <li className="department">
                                                                    <Link to="#"
                                                                        className="fw-semibold"><span>Account
                                                                            Management</span></Link>
                                                                    <ul>
                                                                        <li><Link to="#"><span>Sign
                                                                            Up</span></Link></li>
                                                                        <li><Link
                                                                            to="#"><span>Login</span></Link>
                                                                        </li>
                                                                        <li><Link to="#"><span>Profile
                                                                            Settings</span></Link></li>
                                                                        <li><Link to="#"><span>Modify
                                                                            Reservation</span></Link></li>
                                                                        <li><Link to="#"><span>Cancel
                                                                            Reservation</span></Link></li>
                                                                        <li><Link to="#"><span>Write
                                                                            Reviews</span></Link></li>
                                                                    </ul>
                                                                </li>
                                                                <li className="department">
                                                                    <Link to="#"
                                                                        className="fw-semibold"><span>About Us</span></Link>
                                                                    <ul>
                                                                        <li><Link
                                                                            to="#"><span>Overview</span></Link>
                                                                        </li>
                                                                        <li><Link to="#"><span>Connect
                                                                            Via Social Media</span></Link></li>
                                                                        <li><Link
                                                                            to="#"><span>Careers</span></Link>
                                                                        </li>
                                                                        <li><Link to="#"><span>Team
                                                                            Members</span></Link></li>
                                                                        <li><Link
                                                                            to="#"><span>Policies</span></Link>
                                                                        </li>
                                                                    </ul>
                                                                </li>
                                                                <li className="department">
                                                                    <Link to="#"
                                                                        className="fw-semibold"><span>Book a Trip</span></Link>
                                                                    <ul>
                                                                        <li><Link to="#"><span>Travel
                                                                            Details</span></Link></li>
                                                                        <li><Link to="#"><span>Reservation
                                                                            Process</span></Link></li>
                                                                        <li><Link to="#"><span>Payment
                                                                            Option</span></Link></li>
                                                                        <li><Link
                                                                            to="#"><span>Comfirmation</span></Link>
                                                                        </li>
                                                                    </ul>
                                                                </li>
                                                                <li className="department">
                                                                    <Link to="#"
                                                                        className="fw-semibold"><span>Destination</span></Link>
                                                                    <ul>
                                                                        <li><Link
                                                                            to="#"><span>Architecture</span></Link>
                                                                        </li>
                                                                        <li><Link
                                                                            to="#"><span>Art</span></Link>
                                                                        </li>
                                                                        <li><Link
                                                                            to="#"><span>Entertainment</span></Link>
                                                                        </li>
                                                                        <li><Link
                                                                            to="#"><span>History</span></Link>
                                                                        </li>
                                                                        <li><Link
                                                                            to="#"><span>Science</span></Link>
                                                                        </li>
                                                                        <li><Link
                                                                            to="#"><span>Sports</span></Link>
                                                                        </li>
                                                                        <li><Link
                                                                            to="#"><span>Music</span></Link>
                                                                        </li>
                                                                        <li><Link to="#"><span>Tracking
                                                                            Camp</span></Link></li>
                                                                    </ul>
                                                                </li>
                                                                <li className="department">
                                                                    <Link to="#"
                                                                        className="fw-semibold"><span>Travel Tips</span></Link>
                                                                    <ul>
                                                                        <li><Link to="#"><span>General
                                                                            Travel</span></Link></li>
                                                                        <li><Link to="#"><span>Helpth
                                                                            Concerns</span></Link></li>
                                                                        <li><Link to="#"><span>Safety
                                                                            Measures</span></Link></li>
                                                                        <li><Link
                                                                            to="#"><span>FAQ's</span></Link>
                                                                        </li>
                                                                    </ul>
                                                                </li>
                                                            </ul>
                                                        </li>
                                                    </ul>
                                                </li>
                                            </ul>
                                        </figure>
                                    </div>

                                </CardBody>

                            </Card>

                        </Col>

                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Horizontal</h4>
                                </CardHeader>
                                <CardBody>
                                    <div className="hori-sitemap">
                                        <ul className="list-unstyled mb-0">
                                            <li className="p-0 parent-title"><Link to="#" className="fw-semibold fs-15">My Account</Link></li>
                                            <ul className="list-unstyled row g-0">
                                                <li className="col-sm-3">
                                                    <Link to="#" className="fw-semibold sub-title">About Us</Link>
                                                    <ul className="list-unstyled row g-0 second-list">
                                                        <li className="col-sm-6">
                                                            <Link to="#">Overview</Link>
                                                        </li>
                                                        <li className="col-sm-6">
                                                            <Link to="#">History</Link>
                                                        </li>
                                                    </ul>
                                                </li>
                                                <li className="col-sm-3">
                                                    <Link to="#" className="fw-semibold">My self-care Plan</Link>
                                                    <ul className="list-unstyled second-list pt-0">
                                                        <li>
                                                            <div>
                                                                <Link to="#">Basic</Link>
                                                                <Link to="#">Early Physiotherapy</Link>
                                                                <Link to="#">Intermediate Physiotherapy</Link>
                                                                <Link to="#">Return to Normal Activity</Link>
                                                            </div>
                                                        </li>
                                                    </ul>
                                                </li>
                                                <li className="col-sm-3">
                                                    <Link to="#" className="fw-semibold sub-title">Support Us</Link>
                                                    <ul className="list-unstyled row g-0 sub-list">
                                                        <li className="col-sm-6">
                                                            <Link to="#">Contact Us</Link>
                                                        </li>
                                                        <li className="col-sm-6">
                                                            <Link to="#">Customer Services</Link>
                                                            <ul className="list-unstyled second-list">
                                                                <li>
                                                                    <div>
                                                                        <Link to="#">Chat With Us</Link>
                                                                        <Link to="#">Connect Information</Link>
                                                                        <Link to="#">FAQ'S</Link>
                                                                    </div>
                                                                </li>
                                                            </ul>
                                                        </li>
                                                    </ul>
                                                </li>
                                                <li className="col-sm-3">
                                                    <Link to="#" className="fw-semibold">Terms & Conditions</Link>
                                                </li>
                                            </ul>
                                        </ul>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <CardHeader>
                                    <h4 className="card-title mb-0">Vertical</h4>
                                </CardHeader>
                                <CardBody>
                                    <Row>
                                        <Col sm={6}>
                                            <div className="verti-sitemap">
                                                <ul className="list-unstyled mb-0">
                                                    <li className="p-0 parent-title"><Link to="#" className="fw-medium fs-14">Nancy Martino - Project Director</Link></li>
                                                    <li>
                                                        <div className="first-list">
                                                            <div className="list-wrap">
                                                                <Link to="#" className="fw-medium text-primary">Erica Kernan - Team Leader</Link>
                                                            </div>
                                                            <ul className="second-list list-unstyled">
                                                                <li>
                                                                    <Link to="#">Jason McQuaid -
                                                                        Member</Link>
                                                                </li>
                                                                <li>
                                                                    <Link to="#">Elwood Arter -
                                                                        Member</Link>
                                                                </li>
                                                            </ul>
                                                        </div>
                                                        <div className="first-list">
                                                            <div className="list-wrap">
                                                                <Link to="#"
                                                                    className="fw-medium text-primary">Mary Jones - Project Manager</Link>
                                                            </div>
                                                            <ul className="second-list list-unstyled">
                                                                <li><Link to="#">Jordyn Jones - Designer</Link></li>
                                                                <li><Link to="#">Ashlee Haney - Developer</Link></li>
                                                                <li><Link to="#">Rashad Charles - BackEnd Developer</Link></li>
                                                                <li><Link to="#">Walter Newman - Frontend Developer</Link></li>
                                                                <li><Link to="#">Adam Moss - Designer</Link></li>
                                                            </ul>
                                                        </div>
                                                        <div className="first-list">
                                                            <div className="list-wrap">
                                                                <Link to="#" className="fw-medium text-primary">Tilly Kent - Executive Manager</Link>
                                                            </div>
                                                            <ul className="second-list list-unstyled">
                                                                <li>
                                                                    <Link to="#">Tyler Porter - Account Executive</Link>
                                                                </li>
                                                                <li>
                                                                    <Link to="#">Alicia Thompson - Sales Executive</Link>
                                                                    <ul className="third-list list-unstyled">
                                                                        <li><Link to="#">Jack Coates - Member</Link></li>
                                                                        <li><Link to="#">Owen Jarvis - Member</Link></li>
                                                                        <li><Link to="#">Ashlee Haney - Member</Link></li>
                                                                        <li><Link to="#">Archie Cook - Member</Link></li>
                                                                    </ul>
                                                                </li>
                                                            </ul>
                                                        </div>
                                                        <div className="first-list">
                                                            <div className="list-wrap">
                                                                <Link to="#"
                                                                    className="fw-medium text-primary">Rachel Rose - HR</Link>
                                                            </div>
                                                        </div>
                                                    </li>
                                                </ul>
                                            </div>
                                        </Col>

                                        <Col sm={6}>
                                            <div className="verti-sitemap">
                                                <ul className="mb-0 list-unstyled">
                                                    <li className="p-0 parent-title"><Link to="#" className="fw-medium fs-14">Velzon</Link></li>
                                                    <li>
                                                        <div className="first-list">
                                                            <div className="list-wrap">
                                                                <Link to="#" className="fw-medium text-primary"><i
                                                                    className="ri-airplay-line me-1 align-bottom"></i> Dashboards</Link>
                                                            </div>
                                                            <ul className="second-list list-unstyled">
                                                                <li>
                                                                    <Link to="#">Analytics</Link>
                                                                </li>
                                                                <li>
                                                                    <Link to="#">CRM</Link>
                                                                </li>
                                                            </ul>
                                                        </div>
                                                        <div className="first-list">
                                                            <div className="list-wrap">
                                                                <Link to="#" className="fw-medium text-primary"><i className="ri-pencil-ruler-2-line me-1 align-bottom"></i> App Pages</Link>
                                                            </div>
                                                            <ul className="second-list list-unstyled">
                                                                <li><Link to="#">Calender</Link></li>
                                                                <li><Link to="#">Chat</Link></li>
                                                                <li><Link to="#">Email</Link></li>
                                                                <li><Link to="#">Ecommerce</Link></li>
                                                                <li><Link to="#">Projects</Link></li>
                                                                <li><Link to="#">Tasks</Link></li>
                                                            </ul>
                                                        </div>
                                                        <div className="first-list">
                                                            <div className="list-wrap">
                                                                <Link to="#" className="fw-medium text-primary"><i className="ri-file-list-3-line me-1 align-bottom"></i> Pages</Link>
                                                            </div>
                                                        </div>
                                                        <div className="first-list">
                                                            <div className="list-wrap">
                                                                <Link to="#" className="fw-medium text-primary"><i className="ri-stack-line me-1 align-bottom"></i> Components</Link>
                                                            </div>
                                                            <ul className="second-list list-unstyled">
                                                                <li>
                                                                    <Link to="#">Base UI</Link>
                                                                </li>
                                                                <li>
                                                                    <Link to="#">Advance UI</Link>
                                                                    <ul className="third-list list-unstyled">
                                                                        <li><Link to="#">Sweet Alerts</Link></li>
                                                                        <li><Link to="#">Range Slider</Link></li>
                                                                        <li><Link to="#">Nestable List</Link></li>
                                                                    </ul>
                                                                </li>
                                                            </ul>
                                                        </div>
                                                    </li>
                                                </ul>
                                            </div>
                                        </Col>
                                    </Row>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </div>
    );
};

export default SiteMap;

