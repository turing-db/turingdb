import React, { useState } from 'react';
import { Button, ButtonGroup, Card, CardBody, Col, Container, DropdownItem, DropdownMenu, DropdownToggle, Input, Nav, NavItem, NavLink, Row, TabContent, TabPane, UncontrolledButtonDropdown, UncontrolledDropdown, } from 'reactstrap';
import { Link } from 'react-router-dom';
import classnames from "classnames";
import SimpleBar from 'simplebar-react';
// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Images
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";

import bell from "../../../assets/images/svg/bell.svg";


import { SingleButtonDropdownExample, ColorDropdownExample, SplitButtonDropdownExample, SizingDropdownExample, DarkDropdownExample, AlignDropdownExample, OptionDropdownExample, AutoCloseDropdownExample, MenuItemDropdownExample, MenuContentDropdownExample, NotificationDropdownExample } from './UiDropdownsCode';

const UiDropdowns = () => {
    document.title="Dropdowns | Velzon - React Admin & Dashboard Template";

    // Default Tabs
    const [activeTab, setactiveTab] = useState("1");
    const toggle = (tab) => {
        if (activeTab !== tab) {
            setactiveTab(tab);
        }
    };

    // Notification Tabs
    const [NotificationTab, setNotificationTab] = useState("1");
    const toggleNotification = (tab) => {
        if (NotificationTab !== tab) {
            setNotificationTab(tab);
        }
    };

    // Notification Tabs
    const [Notification1Tab, setNotification1Tab] = useState("1");
    const toggleNotification1 = (tab) => {
        if (Notification1Tab !== tab) {
            setNotification1Tab(tab);
        }
    };

    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Dropdowns" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Single Button Dropdown" />

                                <CardBody>                                    
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3">

                                            <UncontrolledDropdown>
                                                <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuButton">
                                                    Dropdown button <i className="mdi mdi-chevron-down"></i>
                                                </DropdownToggle>
                                                <DropdownMenu>
                                                    <DropdownItem>Action</DropdownItem>
                                                    <DropdownItem>Another action</DropdownItem>
                                                    <DropdownItem>Something else here</DropdownItem>
                                                </DropdownMenu>
                                            </UncontrolledDropdown>

                                            <UncontrolledDropdown>
                                                <DropdownToggle tag="a" className="btn btn-secondary">
                                                    Dropdown link <i className="mdi mdi-chevron-down"></i>
                                                </DropdownToggle>
                                                <DropdownMenu>
                                                    <DropdownItem>Action</DropdownItem>
                                                    <DropdownItem>Another action</DropdownItem>
                                                    <DropdownItem>Something else here</DropdownItem>
                                                </DropdownMenu>
                                            </UncontrolledDropdown>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "375px" }}>
                                            <code>
                                                <SingleButtonDropdownExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Dropdown Color Variant" />
                                <CardBody>
                                    <p className="text-muted">Use <code>btn-</code> class with below mentioned variation to color dropdown toggle.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3">
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-primary">
                                                        Primary <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                            <ButtonGroup>

                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-success">
                                                        Success <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-light">
                                                        Light <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-info">
                                                        Info <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-secondary">
                                                        Secondary <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-warning">
                                                        Warning <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-danger">
                                                        Danger <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-dark">
                                                        Dark <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "375px" }}>
                                            <code>
                                                <ColorDropdownExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Split Button Dropdown" />
                                <CardBody>
                                    <p className="text-muted">Use <code>divider</code> attribute to DropdownItem to create split button dropdowns as a single button dropdown. </p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3">
                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="primary">Primary</Button>
                                                    <DropdownToggle tag="button" className="btn btn-primary" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="success">Success</Button>
                                                    <DropdownToggle tag="button" className="btn btn-success" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="light">Light</Button>
                                                    <DropdownToggle tag="button" className="btn btn-light" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="info">Info</Button>
                                                    <DropdownToggle tag="button" className="btn btn-info" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="secondary">Secondary</Button>
                                                    <DropdownToggle tag="button" className="btn btn-secondary" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="warning">Warning</Button>
                                                    <DropdownToggle tag="button" className="btn btn-warning" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="danger">Danger</Button>
                                                    <DropdownToggle tag="button" className="btn btn-danger" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="dark">Dark</Button>
                                                    <DropdownToggle tag="button" className="btn btn-dark" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "375px" }}>
                                            <code>
                                                <SplitButtonDropdownExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Dropdown Sizing" />


                                <CardBody>
                                    <p className="text-muted">Use <code>btn-lg</code> class to create a large size dropdown button and <code>btn-sm</code> to create a small size dropdown button.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3 align-items-center">

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-primary btn-lg">
                                                        Large button <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="light" className="btn-lg">Large split button</Button>
                                                    <DropdownToggle tag="button" className="btn btn-light btn-lg" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-primary btn-sm">
                                                        Small button <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="light" className="btn-sm">Small split button</Button>
                                                    <DropdownToggle tag="button" className="btn btn-light btn-sm" split>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "375px" }}>
                                            <code>
                                                <SizingDropdownExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Dark Dropdowns" />

                                <CardBody>
                                    <p className="text-muted">Use <code>dropdown-menu-dark</code> class onto an existing dropdown-menu to create dropdown items dark.</p>
                                    <div className="live-preview">
                                        <UncontrolledDropdown>
                                            <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuButton2">
                                                Dropdown button <i className="mdi mdi-chevron-down"></i>
                                            </DropdownToggle>
                                            <DropdownMenu className="dropdown-menu-dark">
                                                <DropdownItem active >Action</DropdownItem>
                                                <DropdownItem>Another action</DropdownItem>
                                                <DropdownItem>Something else here</DropdownItem>
                                                <DropdownItem divider />
                                                <DropdownItem>Separated link</DropdownItem>
                                            </DropdownMenu>
                                        </UncontrolledDropdown>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <DarkDropdownExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Alignment options" />

                                <CardBody>
                                    <p className="text-muted">Dropdown demo with various <code>dropdown alignment</code> options.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3">
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuButton">
                                                        Dropdown <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-secondary">
                                                        Right-aligned menu <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu end={true}>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-secondary">
                                                        Left-aligned, right-aligned lg <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu className="dropdown-menu-lg-end" end={true}>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-secondary">
                                                        Right-aligned, left-aligned lg <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu className="dropdown-menu-lg-start">
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown direction="start">
                                                    <DropdownToggle tag="button" className="btn btn-secondary">
                                                        Dropstart <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown direction="end">
                                                    <DropdownToggle tag="button" className="btn btn-secondary">
                                                        Dropend <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown direction="up">
                                                    <DropdownToggle tag="button" className="btn btn-secondary">
                                                        Dropup<i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <AlignDropdownExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Dropdown Options" />

                                <CardBody>
                                    <p className="text-muted">Use <code>data-bs-offset</code> or <code>data-bs-reference</code> to change the position of the dropdown.</p>

                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3">

                                            <UncontrolledDropdown className="me-1">
                                                <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuOffset">
                                                    Dropup<i className="mdi mdi-chevron-down"></i>
                                                </DropdownToggle>
                                                <DropdownMenu>
                                                    <DropdownItem>Action</DropdownItem>
                                                    <DropdownItem>Another action</DropdownItem>
                                                    <DropdownItem>Something else here</DropdownItem>
                                                </DropdownMenu>
                                            </UncontrolledDropdown>

                                            <ButtonGroup>
                                                <UncontrolledButtonDropdown>
                                                    <Button color="secondary">Reference</Button>
                                                    <DropdownToggle tag="button" className="btn btn-secondary" id="dropdownMenuReference" split>
                                                        <span className="visually-hidden">Toggle Dropdown</span>
                                                    </DropdownToggle>
                                                    <DropdownMenu >
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledButtonDropdown>
                                            </ButtonGroup>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <OptionDropdownExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Auto Close Behavior" />

                                <CardBody>
                                    <p className="text-muted">By default, the dropdown menu is closed when clicking inside or outside the dropdown menu. You can use the <code>autoClose</code> option to change this behavior of the dropdown.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3">
                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-light">
                                                        Default dropdown <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-light">
                                                        Clickable outside <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-light" id="dropdownMenuClickableInside">
                                                        Clickable inside <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-light" id="dropdownMenuClickable">
                                                        Manual close <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                        <DropdownItem>Menu item</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <AutoCloseDropdownExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Dropdown Menu Item Color" />

                                <CardBody>
                                    <p className="text-muted">Example of dropdown menu and dropdown item color.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col xxl={3}>
                                                <div>
                                                    <h6 className="font-size-13 mb-3">Dropdown Menu Success link example</h6>
                                                    <div className="clearfix">
                                                        <div className="dropdown-menu d-inline-block position-relative dropdownmenu-success" style={{ zIndex: "1" }}>
                                                            <Link to="#" className="dropdown-item">Action</Link>
                                                            <Link to="#" className="dropdown-item">Another action</Link>
                                                            <Link to="#" className="dropdown-item active">Something else here</Link>
                                                            <div className="dropdown-divider"></div>
                                                            <Link to="#" className="dropdown-item">Separated link</Link>
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={9}>
                                                <div className="mt-lg-0 mt-3">
                                                    <h6 className="font-size-13 mb-0">Dropdown Menu link Color example</h6>
                                                    <div>
                                                        <Row>
                                                            <Col lg={4} sm={6}>
                                                                <div className="mt-3">
                                                                    <p className="font-size-13 mb-2">Dropdown menu Primary link</p>
                                                                    <ButtonGroup>
                                                                        <UncontrolledDropdown>
                                                                            <DropdownToggle tag="button" className="btn btn-primary">
                                                                                Primary <i className="mdi mdi-chevron-down"></i>
                                                                            </DropdownToggle>
                                                                            <DropdownMenu className="dropdownmenu-primary">
                                                                                <DropdownItem>Action</DropdownItem>
                                                                                <DropdownItem>Another action</DropdownItem>
                                                                                <DropdownItem>Something else here</DropdownItem>
                                                                                <DropdownItem divider />
                                                                                <DropdownItem>Separated link</DropdownItem>
                                                                            </DropdownMenu>
                                                                        </UncontrolledDropdown>
                                                                    </ButtonGroup>
                                                                </div>
                                                            </Col>
                                                            <Col lg={4} sm={6}>
                                                                <div className="mt-3">
                                                                    <p className="font-size-13 mb-2">Dropdown menu Secondary link</p>
                                                                    <ButtonGroup>
                                                                        <UncontrolledDropdown>
                                                                            <DropdownToggle tag="button" className="btn btn-secondary">
                                                                                Secondary <i className="mdi mdi-chevron-down"></i>
                                                                            </DropdownToggle>
                                                                            <DropdownMenu className="dropdownmenu-secondary">
                                                                                <DropdownItem>Action</DropdownItem>
                                                                                <DropdownItem>Another action</DropdownItem>
                                                                                <DropdownItem>Something else here</DropdownItem>
                                                                                <DropdownItem divider />
                                                                                <DropdownItem>Separated link</DropdownItem>
                                                                            </DropdownMenu>
                                                                        </UncontrolledDropdown>
                                                                    </ButtonGroup>
                                                                </div>
                                                            </Col>
                                                            <Col lg={4} sm={6}>
                                                                <div className="mt-3">
                                                                    <p className="font-size-13 mb-2">Dropdown menu Success link</p>
                                                                    <ButtonGroup>
                                                                        <UncontrolledDropdown>
                                                                            <DropdownToggle tag="button" className="btn btn-success">
                                                                                Success <i className="mdi mdi-chevron-down"></i>
                                                                            </DropdownToggle>
                                                                            <DropdownMenu className="dropdownmenu-success">
                                                                                <DropdownItem>Action</DropdownItem>
                                                                                <DropdownItem>Another action</DropdownItem>
                                                                                <DropdownItem>Something else here</DropdownItem>
                                                                                <DropdownItem divider />
                                                                                <DropdownItem>Separated link</DropdownItem>
                                                                            </DropdownMenu>
                                                                        </UncontrolledDropdown>
                                                                    </ButtonGroup>
                                                                </div>
                                                            </Col>
                                                            <Col lg={4} sm={6}>
                                                                <div className="mt-3">
                                                                    <p className="font-size-13 mb-2">Dropdown menu Warning link</p>
                                                                    <ButtonGroup>
                                                                        <UncontrolledDropdown>
                                                                            <DropdownToggle tag="button" className="btn btn-warning">
                                                                                Warning <i className="mdi mdi-chevron-down"></i>
                                                                            </DropdownToggle>
                                                                            <DropdownMenu className="dropdownmenu-warning">
                                                                                <DropdownItem>Action</DropdownItem>
                                                                                <DropdownItem>Another action</DropdownItem>
                                                                                <DropdownItem>Something else here</DropdownItem>
                                                                                <DropdownItem divider />
                                                                                <DropdownItem>Separated link</DropdownItem>
                                                                            </DropdownMenu>
                                                                        </UncontrolledDropdown>
                                                                    </ButtonGroup>
                                                                </div>
                                                            </Col>
                                                            <Col lg={4} sm={6}>
                                                                <div className="mt-3">
                                                                    <p className="font-size-13 mb-2">Dropdown menu Info link</p>
                                                                    <ButtonGroup>
                                                                        <UncontrolledDropdown>
                                                                            <DropdownToggle tag="button" className="btn btn-info">
                                                                                Info <i className="mdi mdi-chevron-down"></i>
                                                                            </DropdownToggle>
                                                                            <DropdownMenu className="dropdownmenu-info">
                                                                                <DropdownItem>Action</DropdownItem>
                                                                                <DropdownItem>Another action</DropdownItem>
                                                                                <DropdownItem>Something else here</DropdownItem>
                                                                                <DropdownItem divider />
                                                                                <DropdownItem>Separated link</DropdownItem>
                                                                            </DropdownMenu>
                                                                        </UncontrolledDropdown>
                                                                    </ButtonGroup>
                                                                </div>
                                                            </Col>
                                                            <Col lg={4} sm={6}>
                                                                <div className="mt-3">
                                                                    <p className="font-size-13 mb-2">Dropdown menu Danger link</p>
                                                                    <ButtonGroup>
                                                                        <UncontrolledDropdown>
                                                                            <DropdownToggle tag="button" className="btn btn-danger">
                                                                                Danger <i className="mdi mdi-chevron-down"></i>
                                                                            </DropdownToggle>
                                                                            <DropdownMenu className="dropdownmenu-danger">
                                                                                <DropdownItem>Action</DropdownItem>
                                                                                <DropdownItem>Another action</DropdownItem>
                                                                                <DropdownItem>Something else here</DropdownItem>
                                                                                <DropdownItem divider />
                                                                                <DropdownItem>Separated link</DropdownItem>
                                                                            </DropdownMenu>
                                                                        </UncontrolledDropdown>
                                                                    </ButtonGroup>
                                                                </div>
                                                            </Col>
                                                        </Row>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <MenuItemDropdownExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Menu Content" />

                                <CardBody>
                                    <p className="text-muted">Example of dropdown menu containing <code>Headers, Text and Forms</code> content.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">

                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-primary">
                                                        Header <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <div className="dropdown-header noti-title">
                                                            <h5 className="font-size-13 text-muted text-truncate mb-0">Welcome Jessie!</h5>
                                                        </div>
                                                        <DropdownItem>Action</DropdownItem>
                                                        <DropdownItem>Another action</DropdownItem>
                                                        <DropdownItem>Something else here</DropdownItem>
                                                        <DropdownItem divider />
                                                        <DropdownItem>Separated link</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>


                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-success">
                                                        Text <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu className="dropdown-menu-md p-3">
                                                        <p>
                                                            Some example text that's free-flowing within the dropdown menu.
                                                        </p>
                                                        <p className="mb-0">
                                                            And this is more example text.
                                                        </p>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>


                                            <ButtonGroup>
                                                <UncontrolledDropdown>
                                                    <DropdownToggle tag="button" className="btn btn-light">
                                                        Forms <i className="mdi mdi-chevron-down"></i>
                                                    </DropdownToggle>
                                                    <DropdownMenu className="dropdown-menu-md p-4">
                                                        <form>
                                                            <div className="mb-2">
                                                                <label className="form-label" htmlFor="exampleDropdownFormEmail">Email address</label>
                                                                <Input type="email" className="form-control" id="exampleDropdownFormEmail" placeholder="email@example.com" />
                                                            </div>
                                                            <div className="mb-2">
                                                                <label className="form-label" htmlFor="exampleDropdownFormPassword">Password</label>
                                                                <Input type="password" className="form-control" id="exampleDropdownFormPassword" placeholder="Password" />
                                                            </div>
                                                            <div className="mb-2">
                                                                <div className="form-check custom-checkbox">
                                                                    <Input type="checkbox" className="form-check-input" id="rememberdropdownCheck" />
                                                                    <label className="form-check-label" htmlFor="rememberdropdownCheck">Remember me</label>
                                                                </div>
                                                            </div>
                                                            <Button type="submit" className="btn btn-primary">Sign in</Button>
                                                        </form>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                            </ButtonGroup>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <MenuContentDropdownExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>


                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Notifications" />
                                <CardBody>
                                    <p className="text-muted">Dropdown with notification containing multiple tabs.</p>
                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <Col xxl={3} lg={5} md={6}>
                                                <div className="dropdown-menu dropdown-menu-lg dropdown-menu-end p-0 d-inline-block position-relative w-100" aria-labelledby="page-header-notifications-dropdown" style={{ zIndex: "1" }}>
                                                    <div className="dropdown-head bg-primary bg-pattern rounded-top">
                                                        <div className="p-3">
                                                            <Row className="align-items-center">
                                                                <Col>
                                                                    <h6 className="m-0 fs-16 fw-semibold text-white"> Notifications </h6>
                                                                </Col>
                                                                <div className="col-auto dropdown-tabs">
                                                                    <span className="badge badge-soft-light fs-13"> 4 New</span>
                                                                </div>
                                                            </Row>
                                                        </div>

                                                        <div className="px-2 pt-2">
                                                            <Nav tabs className="dropdown-tabs nav-tabs-custom">
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "1", })} onClick={() => { toggle("1"); }} >
                                                                        All (4)
                                                                    </NavLink>
                                                                </NavItem>
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "2", })} onClick={() => { toggle("2"); }} >
                                                                        Messages
                                                                    </NavLink>
                                                                </NavItem>
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "3", })} onClick={() => { toggle("3"); }} >
                                                                        Alerts
                                                                    </NavLink>
                                                                </NavItem>
                                                            </Nav>
                                                        </div>
                                                    </div>

                                                    <TabContent activeTab={activeTab} id="notificationItemsTabContent4">
                                                        <TabPane tabId="1" id="all-noti-tab1" className="py-2 ps-2">
                                                            <SimpleBar style={{ maxHeight: "300px" }} className="pe-2">
                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <div className="flex-shrink-0 avatar-xs me-3">
                                                                            <span className="avatar-title bg-soft-info text-info rounded-circle fs-16">
                                                                                <i className="bx bx-badge-check"></i>
                                                                            </span>
                                                                        </div>
                                                                        <div className="flex-1">
                                                                            <h6 className="mt-0 mb-2 lh-base">Your <b>Elite</b> author Graphic Optimization <span className="text-secondary">reward</span> is ready!</h6>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> Just 30 sec ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar2}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Angela Bernier</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Answered to your comment on the cash flow forecast's graph 🔔.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 48 min ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <div className="flex-shrink-0 avatar-xs me-3">
                                                                            <span className="avatar-title bg-soft-danger text-danger rounded-circle fs-16">
                                                                                <i className='bx bx-message-square-dots' ></i>
                                                                            </span>
                                                                        </div>
                                                                        <div className="flex-1">
                                                                            <h6 className="mt-0 mb-2 fs-13 lh-base">You have received <b className="text-success">20</b> new messages in the conversation</h6>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 2 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar8}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Maureen Gibson</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 4 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="my-3 text-center">
                                                                    <button type="button" className="btn btn-soft-success waves-effect waves-light">View All Notifications <i className="ri-arrow-right-line align-middle"></i></button>
                                                                </div>
                                                            </SimpleBar>
                                                        </TabPane>

                                                        <TabPane tabId="2" id="messages-tab1" className="py-2 ps-2">
                                                            <SimpleBar style={{ maxHeight: "300px" }} className="pe-2">
                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar3}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">James Lemire</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 30 min ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar2}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Angela Bernier</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Answered to your comment on the cash flow forecast's graph 🔔.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 2 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar6}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Kenneth Brown</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Mentionned you in his comment on 📃 invoice #12501.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 10 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar8}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Maureen Gibson</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 3 days ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="my-3 text-center">
                                                                    <button type="button" className="btn btn-soft-success waves-effect waves-light">View All Messages <i className="ri-arrow-right-line align-middle"></i></button>
                                                                </div>
                                                            </SimpleBar>
                                                        </TabPane>

                                                        <TabPane tabId="3" id="alerts-tab1" className="p-4">
                                                            <div className="w-50 pt-3 mx-auto">
                                                                <img src={bell} className="img-fluid" alt="user-pic" />
                                                            </div>
                                                            <div className="text-center pb-5 mt-2">
                                                                <h6 className="fs-18 fw-semibold lh-base">Hey! You have no any notifications </h6>
                                                            </div>
                                                        </TabPane>
                                                    </TabContent>
                                                </div>
                                            </Col>

                                            <Col xxl={3} lg={5} md={6}>
                                                <div className="dropdown-menu dropdown-menu-lg dropdown-menu-end p-0 d-inline-block position-relative w-100" aria-labelledby="page-header-notifications-dropdown" style={{ zIndex: "1" }}>
                                                    <div className="dropdown-head bg-success bg-pattern rounded-top">
                                                        <div className="p-3">
                                                            <Row className="align-items-center">
                                                                <Col>
                                                                    <h6 className="m-0 fs-16 fw-semibold text-white"> Notifications </h6>
                                                                </Col>
                                                                <div className="col-auto dropdown-tabs">
                                                                    <span className="badge badge-soft-light fs-13"> 4 New</span>
                                                                </div>
                                                            </Row>
                                                        </div>

                                                        <div className="px-2 pt-2">
                                                            <Nav tabs className="dropdown-tabs nav-tabs-custom">
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: NotificationTab === "1", })} onClick={() => { toggleNotification("1"); }} >
                                                                        All (4)
                                                                    </NavLink>
                                                                </NavItem>
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: NotificationTab === "2", })} onClick={() => { toggleNotification("2"); }} >
                                                                        Messages
                                                                    </NavLink>
                                                                </NavItem>
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: NotificationTab === "3", })} onClick={() => { toggleNotification("3"); }} >
                                                                        Alerts
                                                                    </NavLink>
                                                                </NavItem>
                                                            </Nav>
                                                        </div>
                                                    </div>

                                                    <TabContent activeTab={NotificationTab} id="notificationItemsTabContent1">
                                                        <TabPane tabId="1" id="all-noti-tab2" className="py-2 ps-2">
                                                            <SimpleBar style={{ maxHeight: "300px" }} className="pe-2">
                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <div className="flex-shrink-0 avatar-xs me-3">
                                                                            <span className="avatar-title bg-soft-info text-info rounded-circle fs-16">
                                                                                <i className="bx bx-badge-check"></i>
                                                                            </span>
                                                                        </div>
                                                                        <div className="flex-1">
                                                                            <h6 className="mt-0 mb-2 lh-base">Your <b>Elite</b> author Graphic Optimization <span className="text-secondary">reward</span> is ready!</h6>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> Just 30 sec ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar2}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Angela Bernier</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Answered to your comment on the cash flow forecast's graph 🔔.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 48 min ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <div className="flex-shrink-0 avatar-xs me-3">
                                                                            <span className="avatar-title bg-soft-danger text-danger rounded-circle fs-16">
                                                                                <i className='bx bx-message-square-dots' ></i>
                                                                            </span>
                                                                        </div>
                                                                        <div className="flex-1">
                                                                            <h6 className="mt-0 mb-2 fs-13 lh-base">You have received <b className="text-success">20</b> new messages in the conversation</h6>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 2 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar8}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Maureen Gibson</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 4 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="my-3 text-center">
                                                                    <Button color="success" className="btn btn-soft-success">View All Notifications <i className="ri-arrow-right-line align-middle"></i></Button>
                                                                </div>
                                                            </SimpleBar>
                                                        </TabPane>

                                                        <TabPane tabId="2" id="messages-tab2" className="py-2 ps-2">
                                                            <SimpleBar style={{ maxHeight: "300px" }} className="pe-2">
                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar3}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">James Lemire</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 30 min ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar2}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Angela Bernier</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Answered to your comment on the cash flow forecast's graph 🔔.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 2 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar6}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Kenneth Brown</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Mentionned you in his comment on 📃 invoice #12501.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 10 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar8}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Maureen Gibson</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 3 days ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <Input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="my-3 text-center">
                                                                    <Button color="success" className="btn btn-soft-success">View All Messages <i className="ri-arrow-right-line align-middle"></i></Button>
                                                                </div>
                                                            </SimpleBar>
                                                        </TabPane>

                                                        <TabPane tabId="3" id="alerts-tab2" className="p-4">
                                                            <div className="w-50 pt-3 mx-auto">
                                                                <img src={bell} className="img-fluid" alt="user-pic" />
                                                            </div>
                                                            <div className="text-center pb-5 mt-2">
                                                                <h6 className="fs-18 fw-semibold lh-base">Hey! You have no any notifications </h6>
                                                            </div>
                                                        </TabPane>
                                                    </TabContent>
                                                </div>
                                            </Col>

                                            <Col xxl={3} lg={5} md={6}>
                                                <div className="dropdown-menu dropdown-menu-lg dropdown-menu-end p-0 d-inline-block position-relative w-100" aria-labelledby="page-header-notifications-dropdown" style={{ zIndex: "1" }}>
                                                    <div className="dropdown-head bg-secondary bg-pattern rounded-top">
                                                        <div className="p-3">
                                                            <Row className="align-items-center">
                                                                <Col>
                                                                    <h6 className="m-0 fs-16 fw-semibold text-white"> Notifications </h6>
                                                                </Col>
                                                                <div className="col-auto dropdown-tabs">
                                                                    <span className="badge badge-soft-light fs-13"> 4 New</span>
                                                                </div>
                                                            </Row>
                                                        </div>

                                                        <div className="px-2 pt-2">
                                                            <Nav tabs className="dropdown-tabs nav-tabs-custom">
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: Notification1Tab === "1", })} onClick={() => { toggleNotification1("1"); }} >
                                                                        All (4)
                                                                    </NavLink>
                                                                </NavItem>
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: Notification1Tab === "2", })} onClick={() => { toggleNotification1("2"); }} >
                                                                        Messages
                                                                    </NavLink>
                                                                </NavItem>
                                                                <NavItem>
                                                                    <NavLink style={{ cursor: "pointer" }} className={classnames({ active: Notification1Tab === "3", })} onClick={() => { toggleNotification1("3"); }} >
                                                                        Alerts
                                                                    </NavLink>
                                                                </NavItem>
                                                            </Nav>
                                                        </div>
                                                    </div>

                                                    <TabContent activeTab={Notification1Tab} id="notificationItemsTabContent3">
                                                        <TabPane tabId="1" id="all-noti-tab1" className="py-2 ps-2">
                                                            <SimpleBar style={{ maxHeight: "300px" }} className="pe-2">
                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <div className="flex-shrink-0 avatar-xs me-3">
                                                                            <span className="avatar-title bg-soft-info text-info rounded-circle fs-16">
                                                                                <i className="bx bx-badge-check"></i>
                                                                            </span>
                                                                        </div>
                                                                        <div className="flex-1">
                                                                            <h6 className="mt-0 mb-2 lh-base">Your <b>Elite</b> author Graphic Optimization <span className="text-secondary">reward</span> is ready!</h6>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> Just 30 sec ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar2}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Angela Bernier</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Answered to your comment on the cash flow forecast's graph 🔔.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 48 min ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <div className="flex-shrink-0 avatar-xs me-3">
                                                                            <span className="avatar-title bg-soft-danger text-danger rounded-circle fs-16">
                                                                                <i className='bx bx-message-square-dots' ></i>
                                                                            </span>
                                                                        </div>
                                                                        <div className="flex-1">
                                                                            <h6 className="mt-0 mb-2 fs-13 lh-base">You have received <b className="text-success">20</b> new messages in the conversation</h6>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 2 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar8}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Maureen Gibson</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 4 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="my-3 text-center">
                                                                    <Button color="success" className="btn btn-soft-success">View All Notifications <i className="ri-arrow-right-line align-middle"></i></Button>
                                                                </div>
                                                            </SimpleBar>
                                                        </TabPane>

                                                        <TabPane tabId="2" id="messages-tab3" className="py-2 ps-2">
                                                            <SimpleBar style={{ maxHeight: "300px" }} className="pe-2">
                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar3}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">James Lemire</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 30 min ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar2}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Angela Bernier</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Answered to your comment on the cash flow forecast's graph 🔔.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 2 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar6}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Kenneth Brown</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">Mentionned you in his comment on 📃 invoice #12501.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 10 hrs ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="text-reset notification-item d-block dropdown-item">
                                                                    <div className="d-flex">
                                                                        <img src={avatar8}
                                                                            className="me-3 rounded-circle avatar-xs" alt="user-pic" />
                                                                        <div className="flex-1">
                                                                            <Link to="#"><h6 className="mt-0 mb-1 fs-13 fw-semibold">Maureen Gibson</h6></Link>
                                                                            <div className="fs-13 text-muted">
                                                                                <p className="mb-1">We talked about a project on linkedin.</p>
                                                                            </div>
                                                                            <p className="mb-0 fs-11 fw-medium text-uppercase text-muted">
                                                                                <span><i className="mdi mdi-clock-outline"></i> 3 days ago</span>
                                                                            </p>
                                                                        </div>
                                                                        <div className="px-2 fs-16">
                                                                            <input className="form-check-input" type="checkbox" />
                                                                        </div>
                                                                    </div>
                                                                </div>

                                                                <div className="my-3 text-center">
                                                                    <Button color="success" className="btn btn-soft-success">View All Messages <i className="ri-arrow-right-line align-middle"></i></Button>
                                                                </div>
                                                            </SimpleBar>
                                                        </TabPane>

                                                        <TabPane tabId="3" id="alerts-tab3" className="p-4">
                                                            <div className="w-50 pt-3 mx-auto">
                                                                <img src={bell} className="img-fluid" alt="user-pic" />
                                                            </div>
                                                            <div className="text-center pb-5 mt-2">
                                                                <h6 className="fs-18 fw-semibold lh-base">Hey! You have no any notifications </h6>
                                                            </div>
                                                        </TabPane>
                                                    </TabContent>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <NotificationDropdownExample />
                                            </code>
                                        </pre>
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

export default UiDropdowns;
