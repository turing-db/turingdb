import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Card, CardBody, Col, DropdownItem, DropdownMenu, DropdownToggle, Input, Modal, Row, UncontrolledDropdown, ModalBody, ModalHeader } from 'reactstrap';

//SimpleBar
import SimpleBar from "simplebar-react";

//import images
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";

import avatar7 from "../../../assets/images/users/avatar-7.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";

const TeamTab = () => {
    //Modal  
    const [modal, setModal] = useState(false);
    const openModal = () => setModal(!modal);
    return (
        <React.Fragment>
            <Row className="g-4 mb-3">
                <div className="col-sm">
                    <div className="d-flex">
                        <div className="search-box me-2">
                            <input type="text" className="form-control" placeholder="Search member..." />
                            <i className="ri-search-line search-icon"></i>
                        </div>
                    </div>
                </div>
                <div className="col-sm-auto">
                    <div>
                        <button type="button" className="btn btn-danger" onClick={openModal}><i className="ri-share-line me-1 align-bottom"></i> Invite Member</button>
                    </div>
                </div>
            </Row>

            <div className="team-list list-view-filter">
                <Card className="team-box">
                    <CardBody className="px-4">
                        <Row className="align-items-center team-row">
                            <div className="col team-settings">
                                <Row className="align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </Row>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <img src={avatar2} alt="" className="img-fluid d-block rounded-circle" />
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Nancy Martino</h5></Link>
                                        <p className="text-muted mb-0">Team Leader & HR</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <Row className="text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">225</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">197</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </Row>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </Row>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <Row className="align-items-center team-row">
                            <div className="col team-settings">
                                <Row className="align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn active">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </Row>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <div className="avatar-title bg-soft-danger text-danger rounded-circle">
                                            HB
                                        </div>
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Henry Baird</h5></Link>
                                        <p className="text-muted mb-0">Full Stack Developer</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <Row className="text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">352</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">376</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </Row>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </Row>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <Row className="align-items-center team-row">
                            <div className="col team-settings">
                                <Row className="align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn active">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </Row>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <img src={avatar3} alt="" className="img-fluid d-block rounded-circle" />
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Frank Hook</h5></Link>
                                        <p className="text-muted mb-0">Project Manager</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <Row className="text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">164</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">182</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </Row>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </Row>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <Row className="align-items-center team-row">
                            <div className="col team-settings">
                                <Row className="align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </Row>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <img src={avatar8} alt="" className="img-fluid d-block rounded-circle" />
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Jennifer Carter</h5></Link>
                                        <p className="text-muted mb-0">UI/UX Designer</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <Row className="text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">225</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">197</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </Row>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </Row>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <Row className="align-items-center team-row">
                            <div className="col team-settings">
                                <Row className="align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </Row>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <div className="avatar-title bg-soft-success text-success rounded-circle">
                                            ME
                                        </div>
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Megan Elmore</h5></Link>
                                        <p className="text-muted mb-0">Team Leader & Web Developer</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <Row className="text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">201</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">263</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </Row>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </Row>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <Row className="align-items-center team-row">
                            <div className="col team-settings">
                                <Row className="align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </Row>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <img src={avatar4} alt="" className="img-fluid d-block rounded-circle" />
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Alexis Clarke</h5></Link>
                                        <p className="text-muted mb-0">Backend Developer</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <Row className="text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">132</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">147</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </Row>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </Row>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <div className="row align-items-center team-row">
                            <div className="col team-settings">
                                <div className="row align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </div>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <div className="avatar-title bg-soft-info text-info rounded-circle">
                                            NC
                                        </div>
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Nathan Cole</h5></Link>
                                        <p className="text-muted mb-0">Front-End Developer</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <div className="row text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">352</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">376</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </div>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </div>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <div className="row align-items-center team-row">
                            <div className="col team-settings">
                                <div className="row align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </div>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <img src={avatar7} alt="" className="img-fluid d-block rounded-circle" />
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Joseph Parker</h5></Link>
                                        <p className="text-muted mb-0">Team Leader & HR</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <div className="row text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">64</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">93</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </div>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </div>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <div className="row align-items-center team-row">
                            <div className="col team-settings">
                                <div className="row align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </div>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <img src={avatar5} alt="" className="img-fluid d-block rounded-circle" />
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Erica Kernan</h5></Link>
                                        <p className="text-muted mb-0">Web Designer</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <div className="row text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">345</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">298</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </div>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </div>
                    </CardBody>
                </Card>
                <Card className="team-box">
                    <CardBody className="px-4">
                        <div className="row align-items-center team-row">
                            <div className="col team-settings">
                                <div className="row align-items-center">
                                    <div className="col">
                                        <div className="flex-shrink-0 me-2">
                                            <button type="button" className="btn fs-16 p-0 favourite-btn">
                                                <i className="ri-star-fill"></i>
                                            </button>
                                        </div>
                                    </div>
                                    <UncontrolledDropdown className="col text-end">
                                        <DropdownToggle tag="a" role="button">
                                            <i className="ri-more-fill fs-17"></i>
                                        </DropdownToggle>
                                        <DropdownMenu className="dropdown-menu-end">
                                            <li><DropdownItem><i className="ri-eye-fill text-muted me-2 align-bottom"></i>View</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-star-fill text-muted me-2 align-bottom"></i>Favourite</DropdownItem></li>
                                            <li><DropdownItem><i className="ri-delete-bin-5-fill text-muted me-2 align-bottom"></i>Delete</DropdownItem></li>
                                        </DropdownMenu>
                                    </UncontrolledDropdown>
                                </div>
                            </div>
                            <Col lg={4}>
                                <div className="team-profile-img">
                                    <div className="avatar-lg img-thumbnail rounded-circle">
                                        <div className="avatar-title border bg-light text-primary rounded-circle">
                                            DP
                                        </div>
                                    </div>
                                    <div className="team-content">
                                        <Link to="#" className="d-block"><h5 className="fs-16 mb-1">Donald Palmer</h5></Link>
                                        <p className="text-muted mb-0">Wed Developer</p>
                                    </div>
                                </div>
                            </Col>
                            <Col lg={4}>
                                <div className="row text-muted text-center">
                                    <Col xs={6} className="border-end border-end-dashed">
                                        <h5 className="mb-1">97</h5>
                                        <p className="text-muted mb-0">Projects</p>
                                    </Col>
                                    <Col xs={6}>
                                        <h5 className="mb-1">135</h5>
                                        <p className="text-muted mb-0">Tasks</p>
                                    </Col>
                                </div>
                            </Col>
                            <Col lg={2} className="col">
                                <div className="text-end">
                                    <Link to="/pages-profile" className="btn btn-light view-btn">View Profile</Link>
                                </div>
                            </Col>
                        </div>
                    </CardBody>
                </Card>
            </div>


            <div className="row g-0 text-center text-sm-start align-items-center mb-3">
                <div className="col-sm-6">
                    <div>
                        <p className="mb-sm-0">Showing 1 to 10 of 12 entries</p>
                    </div>
                </div>
                <div className="col-sm-6">
                    <ul className="pagination pagination-separated justify-content-center justify-content-sm-end mb-sm-0">
                        <li className="page-item disabled"> <Link to="#" className="page-link"><i className="mdi mdi-chevron-left"></i></Link> </li>
                        <li className="page-item"> <Link to="#" className="page-link">1</Link> </li>
                        <li className="page-item active"> <Link to="#" className="page-link">2</Link> </li>
                        <li className="page-item"> <Link to="#" className="page-link">3</Link> </li>
                        <li className="page-item"> <Link to="#" className="page-link">4</Link> </li>
                        <li className="page-item"> <Link to="#" className="page-link">5</Link> </li>
                        <li className="page-item"> <Link to="#" className="page-link"><i className="mdi mdi-chevron-right"></i></Link> </li>
                    </ul>
                </div>
            </div>

            <Modal isOpen={modal} toggle={openModal} centered className="border-0">
                <ModalHeader toggle={openModal} className="p-3 ps-4 bg-soft-success">
                    Members
                </ModalHeader>
                <ModalBody className="p-4">
                    <div className="search-box mb-3">
                        <Input type="text" className="form-control bg-light border-light" placeholder="Search here..." />
                        <i className="ri-search-line search-icon"></i>
                    </div>

                    <div className="mb-4 d-flex align-items-center">
                        <div className="me-2">
                            <h5 className="mb-0 fs-13">Members :</h5>
                        </div>
                        <div className="avatar-group justify-content-center">
                            <Link to="#" className="avatar-group-item" data-bs-toggle="tooltip" data-bs-trigger="hover" data-bs-placement="top" title="" data-bs-original-title="Brent Gonzalez">
                                <div className="avatar-xs">
                                    <img src={avatar3} alt="" className="rounded-circle img-fluid" />
                                </div>
                            </Link>
                            <Link to="#" className="avatar-group-item" data-bs-toggle="tooltip" data-bs-trigger="hover" data-bs-placement="top" title="" data-bs-original-title="Sylvia Wright">
                                <div className="avatar-xs">
                                    <div className="avatar-title rounded-circle bg-secondary">
                                        S
                                    </div>
                                </div>
                            </Link>
                            <Link to="#" className="avatar-group-item" data-bs-toggle="tooltip" data-bs-trigger="hover" data-bs-placement="top" title="" data-bs-original-title="Ellen Smith">
                                <div className="avatar-xs">
                                    <img src={avatar4} alt="" className="rounded-circle img-fluid" />
                                </div>
                            </Link>
                        </div>
                    </div>
                    <SimpleBar className="mx-n4 px-4" data-simplebar="init" style={{ maxHeight: "225px" }}>
                        <div className="vstack gap-3">
                            <div className="d-flex align-items-center">
                                <div className="avatar-xs flex-shrink-0 me-3">
                                    <img src={avatar2} alt="" className="img-fluid rounded-circle" />
                                </div>
                                <div className="flex-grow-1">
                                    <h5 className="fs-13 mb-0"><Link to="#" className="text-body d-block">Nancy Martino</Link></h5>
                                </div>
                                <div className="flex-shrink-0">
                                    <button type="button" className="btn btn-light btn-sm">Add</button>
                                </div>
                            </div>

                            <div className="d-flex align-items-center">
                                <div className="avatar-xs flex-shrink-0 me-3">
                                    <div className="avatar-title bg-soft-danger text-danger rounded-circle">
                                        HB
                                    </div>
                                </div>
                                <div className="flex-grow-1">
                                    <h5 className="fs-13 mb-0"><Link to="#" className="text-body d-block">Henry Baird</Link></h5>
                                </div>
                                <div className="flex-shrink-0">
                                    <button type="button" className="btn btn-light btn-sm">Add</button>
                                </div>
                            </div>

                            <div className="d-flex align-items-center">
                                <div className="avatar-xs flex-shrink-0 me-3">
                                    <img src={avatar3} alt="" className="img-fluid rounded-circle" />
                                </div>
                                <div className="flex-grow-1">
                                    <h5 className="fs-13 mb-0"><Link to="#" className="text-body d-block">Frank Hook</Link></h5>
                                </div>
                                <div className="flex-shrink-0">
                                    <button type="button" className="btn btn-light btn-sm">Add</button>
                                </div>
                            </div>

                            <div className="d-flex align-items-center">
                                <div className="avatar-xs flex-shrink-0 me-3">
                                    <img src={avatar4} alt="" className="img-fluid rounded-circle" />
                                </div>
                                <div className="flex-grow-1">
                                    <h5 className="fs-13 mb-0"><Link to="#" className="text-body d-block">Jennifer Carter</Link></h5>
                                </div>
                                <div className="flex-shrink-0">
                                    <button type="button" className="btn btn-light btn-sm">Add</button>
                                </div>
                            </div>

                            <div className="d-flex align-items-center">
                                <div className="avatar-xs flex-shrink-0 me-3">
                                    <div className="avatar-title bg-soft-success text-success rounded-circle">
                                        AC
                                    </div>
                                </div>
                                <div className="flex-grow-1">
                                    <h5 className="fs-13 mb-0"><Link to="#" className="text-body d-block">Alexis Clarke</Link></h5>
                                </div>
                                <div className="flex-shrink-0">
                                    <button type="button" className="btn btn-light btn-sm">Add</button>
                                </div>
                            </div>

                            <div className="d-flex align-items-center">
                                <div className="avatar-xs flex-shrink-0 me-3">
                                    <img src={avatar7} alt="" className="img-fluid rounded-circle" />
                                </div>
                                <div className="flex-grow-1">
                                    <h5 className="fs-13 mb-0"><Link to="#" className="text-body d-block">Joseph Parker</Link></h5>
                                </div>
                                <div className="flex-shrink-0">
                                    <button type="button" className="btn btn-light btn-sm">Add</button>
                                </div>
                            </div>
                        </div>

                    </SimpleBar>
                </ModalBody>
                <div className="modal-footer">
                    <button type="button" className="btn btn-light w-xs" data-bs-dismiss="modal">Cancel</button>
                    <button type="button" className="btn btn-success w-xs">Invite</button>
                </div>

            </Modal>
        </React.Fragment>
    );
};

export default TeamTab;