import React, { useState } from 'react';
import { Card, CardBody, Col, Container, Nav, NavItem, NavLink, Row, TabContent, TabPane, UncontrolledTooltip } from "reactstrap";
import { Link } from 'react-router-dom';
import classnames from "classnames";

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

// Import Images
import avatar1 from "../../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../../assets/images/users/avatar-7.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";

import img3 from "../../../assets/images/small/img-3.jpg";
import img4 from "../../../assets/images/small/img-4.jpg";
import img5 from "../../../assets/images/small/img-5.jpg";
import img6 from "../../../assets/images/small/img-6.jpg";
import img7 from "../../../assets/images/small/img-7.jpg";
import img8 from "../../../assets/images/small/img-8.jpg";


const UiTabs = () => {
    document.title="Tabs | Velzon - React Admin & Dashboard Template";

    // Default Tabs
    const [activeTab, setactiveTab] = useState("1");
    const toggle = (tab) => {
        if (activeTab !== tab) {
            setactiveTab(tab);
        }
    };

    // Justify Tabs
    const [justifyTab, setjustifyTab] = useState("1");
    const justifyToggle = (tab) => {
        if (justifyTab !== tab) {
            setjustifyTab(tab);
        }
    };

    // Pills Tabs
    const [pillsTab, setpillsTab] = useState("1");
    const pillsToggle = (tab) => {
        if (pillsTab !== tab) {
            setpillsTab(tab);
        }
    };

    // Pills Justified Tabs
    const [justifyPillsTab, setjustifyPillsTab] = useState("1");
    const justifyPillsToggle = (tab) => {
        if (justifyPillsTab !== tab) {
            setjustifyPillsTab(tab);
        }
    };

    // Vertical Nav Tabs
    const [verticalTab, setverticalTab] = useState("1");
    const toggleVertical = (tab) => {
        if (verticalTab !== tab) {
            setverticalTab(tab);
        }
    };

    // Card Header Tabs
    const [cardHeaderTab, setcardHeaderTab] = useState("1");
    const cardHeaderToggle = (tab) => {
        if (cardHeaderTab !== tab) {
            setcardHeaderTab(tab);
        }
    };

    // Header Justify Tabs
    const [headerJustifyTab, setheaderJustifyTab] = useState("1");
    const headerJustifyToggle = (tab) => {
        if (headerJustifyTab !== tab) {
            setheaderJustifyTab(tab);
        }
    };

    // Custom Tabs Bordered
    const [customActiveTab, setcustomActiveTab] = useState("1");
    const toggleCustom = (tab) => {
        if (customActiveTab !== tab) {
            setcustomActiveTab(tab);
        }
    };

    // Arrow Nav tabs
    const [arrowNavTab, setarrowNavTab] = useState("1");
    const arrowNavToggle = (tab) => {
        if (arrowNavTab !== tab) {
            setarrowNavTab(tab);
        }
    };

    // Bottom Card Nav Tabs
    const [bottomCardTab, setbottomCardTab] = useState("1");
    const bottonCardToggle = (tab) => {
        if (bottomCardTab !== tab) {
            setbottomCardTab(tab);
        }
    };

    // Custom Hover Tabs
    const [customHoverTab, setcustomHoverTab] = useState("1");
    const customHovertoggle = (tab) => {
        if (customHoverTab !== tab) {
            setcustomHoverTab(tab);
        }
    };

    // Custom Vertical Tabs

    const [customverticalTab, setcustomverticalTab] = useState("1");
    const customtoggleVertical = (tab) => {
        if (customverticalTab !== tab) {
            setcustomverticalTab(tab);
        }
    };

    // Animation Nav

    const [animationNavTab, setanimationNavTab] = useState("1");
    const animationNavToggle = (tab) => {
        if (animationNavTab !== tab) {
            setanimationNavTab(tab);
        }
    };

    // Nav With Badge
    const [navBadgeTab, setnavBadgeTab] = useState("1");
    const navBadgeToggle = (tab) => {
        if (navBadgeTab !== tab) {
            setnavBadgeTab(tab);
        }
    };

    // Border Top Nav
    const [topBorderTab, settopBorderTab] = useState("1");
    const topBordertoggle = (tab) => {
        if (topBorderTab !== tab) {
            settopBorderTab(tab);
        }
    };

    // Border Top Nav Justified Tabs
    const [topBorderjustifyTab, settopBorderjustifyTab] = useState("1");
    const topBorderJustifytoggle = (tab) => {
        if (topBorderjustifyTab !== tab) {
            settopBorderjustifyTab(tab);
        }
    };

    // Colored Nav
    const [colorNavTab, setcolorNavTab] = useState("1");
    const colorNavToggle = (tab) => {
        if (colorNavTab !== tab) {
            setcolorNavTab(tab);
        }
    };

    // Light Nav
    const [lightNavTab, setlightNavTab] = useState("1");
    const lightNavToggle = (tab) => {
        if (lightNavTab !== tab) {
            setlightNavTab(tab);
        }
    };
    
    // Outline Border Nav Tabs
    const [outlineBorderNav, setoutlineBorderNav] = useState("1");
    const outlineBorderNavtoggle = (tab) => {
        if (outlineBorderNav !== tab) {
            setoutlineBorderNav(tab);
        }
    };

    // Custom Nav Tabs
    const [customNav, setcustomNav] = useState("1");
    const customNavtoggle = (tab) => {
        if (customNav !== tab) {
            setcustomNav(tab);
        }
    };

    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">              
                <Container fluid>
                    <BreadCrumb title="Tabs" pageTitle="Base UI" />
                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Default Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-tabs</code> class to generate a default tabbed interface.</p>

                                    <Nav tabs className="nav-tabs mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "1", })} onClick={() => { toggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "2", })} onClick={() => { toggle("2"); }} >
                                                Product
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "3", })} onClick={() => { toggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: activeTab === "4", })} onClick={() => { toggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={activeTab} className="text-muted">
                                        <TabPane tabId="1" id="home">
                                            <h6>Graphic Design</h6>
                                            <p className="mb-0">
                                                They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. That's why it's important to think about your message, then choose a font that fits. Cosby sweater eu banh mi, qui irure terry richardson ex squid.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="2" id="product">
                                            <h6>Product</h6>
                                            <p className="mb-0">
                                                You've probably heard that opposites attract. The same is true for fonts. Don't be afraid to combine font styles that are different but complementary, like sans serif with serif, short with tall, or decorative with simple. Qui photo booth letterpress, commodo enim craft beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="3" id="messages">
                                            <h6>Messages</h6>
                                            <p className="mb-0">
                                                Etsy mixtape wayfarers, ethical wes anderson tofu before they
                                                sold out mcsweeney's organic lomo retro fanny pack lo-fi
                                                farm-to-table readymade. Messenger bag gentrify pitchfork
                                                tattooed craft beer, iphone skateboard locavore carles etsy
                                                salvia banksy hoodie helvetica. DIY synth PBR banksy irony.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="4" id="settings">
                                            <h6>Settings</h6>
                                            <p className="mb-0">
                                                Trust fund seitan letterpress, keytar raw denim keffiyeh etsy
                                                art party before they sold out master cleanse gluten-free squid
                                                scenester freegan cosby sweater. Fanny pack portland seitan DIY,
                                                art party locavore wolf cliche high life echo park Austin. Cred
                                                vinyl keffiyeh DIY salvia PBR, banh mi before they sold out
                                                farm-to-table VHS.
                                            </p>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={6}>
                            <h5 className="mb-3">Justify Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-justified</code> class to generate equal-width elements with all horizontal space will be occupied by nav links.</p>

                                    <Nav tabs className="nav-tabs nav-justified mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: justifyTab === "1", })} onClick={() => { justifyToggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: justifyTab === "2", })} onClick={() => { justifyToggle("2"); }} >
                                                Product
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: justifyTab === "3", })} onClick={() => { justifyToggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: justifyTab === "4", })} onClick={() => { justifyToggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={justifyTab} className="text-muted">
                                        <TabPane tabId="1" id="base-justified-home">
                                            <h6>Graphic Design</h6>
                                            <p className="mb-0">
                                                They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. That's why it's important to think about your message, then choose a font that fits. Cosby sweater eu banh mi, qui irure terry richardson ex squid.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="2" id="product">
                                            <h6>Product</h6>
                                            <p className="mb-0">
                                                You've probably heard that opposites attract. The same is true for fonts. Don't be afraid to combine font styles that are different but complementary, like sans serif with serif, short with tall, or decorative with simple. Qui photo booth letterpress, commodo enim craft beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="3" id="base-justified-messages" >
                                            <h6>Messages</h6>
                                            <p className="mb-0">
                                                Etsy mixtape wayfarers, ethical wes anderson tofu before they
                                                sold out mcsweeney's organic lomo retro fanny pack lo-fi
                                                farm-to-table readymade. Messenger bag gentrify pitchfork
                                                tattooed craft beer, iphone skateboard locavore carles etsy
                                                salvia banksy hoodie helvetica. DIY synth PBR banksy irony.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="4" id="base-justified-settings">
                                            <h6>Settings</h6>
                                            <p className="mb-0">
                                                Trust fund seitan letterpress, keytar raw denim keffiyeh etsy
                                                art party before they sold out master cleanse gluten-free squid
                                                scenester freegan cosby sweater. Fanny pack portland seitan DIY,
                                                art party locavore wolf cliche high life echo park Austin. Cred
                                                vinyl keffiyeh DIY salvia PBR, banh mi before they sold out
                                                farm-to-table VHS.
                                            </p>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Pills Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-pills</code> class to generate particular selected nav links without borders.</p>

                                    <Nav pills className="nav-success mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: pillsTab === "1", })} onClick={() => { pillsToggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: pillsTab === "2", })} onClick={() => { pillsToggle("2"); }} >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: pillsTab === "3", })} onClick={() => { pillsToggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: pillsTab === "4", })} onClick={() => { pillsToggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={pillsTab} className="text-muted">
                                        <TabPane tabId="1" id="home-1">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Raw denim you probably haven't heard of them jean shorts Austin.
                                                    Nesciunt tofu stumptown aliqua, retro synth master cleanse.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="profile-1">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    In some designs, you might adjust your tracking to create a certain artistic effect. It can also help you fix fonts that are poorly spaced to begin with.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring which I enjoy with my whole heart.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="messages-1" >
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your own style choices.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="4" id="settings-1">
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    After gathering lots of different opinions and graphic design basics, I came up with a list of 30 graphic design tips that you can start implementing.
                                                </div>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Pills Justified Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-pills nav-justified</code> class to generate equal-width elements without borders, all horizontal space will be occupied by nav links.</p>

                                    <Nav pills className="nav-justified mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: justifyPillsTab === "1", })} onClick={() => { justifyPillsToggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: justifyPillsTab === "2", })} onClick={() => { justifyPillsToggle("2"); }} >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: justifyPillsTab === "3", })} onClick={() => { justifyPillsToggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: justifyPillsTab === "4", })} onClick={() => { justifyPillsToggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={justifyPillsTab} className="text-muted">
                                        <TabPane tabId="1" id="pill-justified-home-1">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Raw denim you probably haven't heard of them jean shorts Austin.
                                                    Nesciunt tofu stumptown aliqua, retro synth master cleanse.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="pill-justified-profile-1">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    In some designs, you might adjust your tracking to create a certain artistic effect. It can also help you fix fonts that are poorly spaced to begin with.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring which I enjoy with my whole heart.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="pill-justified-messages-1" >
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your own style choices.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="4" id="pill-justified-settings-1">
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    After gathering lots of different opinions and graphic design basics, I came up with a list of 30 graphic design tips that you can start implementing.
                                                </div>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Vertical Nav Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>flex-column</code> class to create Vertical nav tabs.</p>
                                    <Row>
                                        <Col md={3}>
                                            <Nav pills className="flex-column" id="v-pills-tab">
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            "mb-2": true,
                                                            active: verticalTab === "1",
                                                        })}
                                                        onClick={() => {
                                                            toggleVertical("1");
                                                        }}
                                                        id="v-pills-home-tab"
                                                    >
                                                        Home
                                                    </NavLink>
                                                </NavItem>
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            "mb-2": true,
                                                            active: verticalTab === "2",
                                                        })}
                                                        onClick={() => {
                                                            toggleVertical("2");
                                                        }}
                                                        id="v-pills-profile-tab"
                                                    >
                                                        Profile
                                                    </NavLink>
                                                </NavItem>
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            "mb-2": true,
                                                            active: verticalTab === "3",
                                                        })}
                                                        onClick={() => {
                                                            toggleVertical("3");
                                                        }}
                                                        id="v-pills-messages-tab"
                                                    >
                                                        Messages
                                                    </NavLink>
                                                </NavItem>
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            active: verticalTab === "4",
                                                        })}
                                                        onClick={() => {
                                                            toggleVertical("4");
                                                        }}
                                                        id="v-pills-settings-tab"
                                                    >
                                                        Settings
                                                    </NavLink>
                                                </NavItem>
                                            </Nav>
                                        </Col>
                                        <Col md={9}>
                                            <TabContent
                                                activeTab={verticalTab}
                                                className="text-muted mt-4 mt-md-0"
                                                id="v-pills-tabContent"
                                            >
                                                <TabPane tabId="1" id="v-pills-home">
                                                    <div className="d-flex mb-2">
                                                        <div className="flex-shrink-0">
                                                            <img src={img4} alt="" width="150" className="rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <p className="mb-0">You always want to make sure that your fonts work well together and try to limit the number of fonts you use to three or less. Experiment and play around with the fonts that you already have in the software you’re working with reputable font websites.</p>
                                                        </div>
                                                    </div>
                                                    <p className="mb-0">
                                                        This may be the most commonly encountered tip I received from the designers I spoke with. They highly encourage that you use different fonts in one design, but do not over-exaggerate and go overboard.
                                                    </p>
                                                </TabPane>
                                                <TabPane tabId="2" id="v-pills-profile">
                                                    <div className="d-flex mb-2">
                                                        <div className="flex-shrink-0">
                                                            <img src={img5} alt="" width="150" className="rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <p className="mb-0"> I also decreased the transparency in the text so that the mountains come through the text, bringing the quote truly to life. Make sure that the placement of your text is pleasing to look at, and you try to achieve symmetry for this effect.</p>
                                                        </div>
                                                    </div>
                                                    <p className="mb-0">
                                                        You've probably heard that opposites attract. The same is true for fonts. Don't be afraid to combine font styles that are different but complementary. You can always play around with the text that is overlaid on an image.
                                                    </p>

                                                </TabPane>
                                                <TabPane tabId="3" id="v-pills-messages">
                                                    <div className="d-flex mb-2">
                                                        <div className="flex-shrink-0">
                                                            <img src={img6} alt="" width="150" className="rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <p className="mb-0">In this image, you can see that the line height has been reduced significantly, and the size was brought up exponentially. Experiment and play around with the fonts that you already have in the software you’re working with reputable font websites.</p>
                                                        </div>
                                                    </div>
                                                    <p className="mb-0">
                                                        They highly encourage that you use different fonts in one design, but do not over-exaggerate and go overboard This may be the most commonly encountered tip I received from the designers I spoke with.
                                                    </p>
                                                </TabPane>

                                                <TabPane tabId="4" id="v-pills-settings">
                                                    <div className="d-flex mb-2">
                                                        <div className="flex-shrink-0">
                                                            <img src={img7} alt="" width="150" className="rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <p className="mb-0">When designing, the goal is to draw someone’s attention and portray to them what you’re trying to say. You can make a big statement by using little tricks, like this one. Use contrasting fonts. you can use a bold sanserif font with a cursive.</p>
                                                        </div>
                                                    </div>
                                                    <p className="mb-0">
                                                        If you’re using multiple elements, make sure that your principal object is larger than the others, as the eye of your viewer will automatically be drawn to the larger of the two objects.
                                                    </p>
                                                </TabPane>
                                            </TabContent>
                                        </Col>
                                    </Row>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Card Header Tabs</h5>
                            <Card>
                                <div className="card-header align-items-center d-flex">
                                    <div className="flex-grow-1 oveflow-hidden">
                                        <p className="text-muted text-truncates mb-0">Use <code>card-header-tabs</code> class to create card header tabs.</p>
                                    </div>
                                    <div className="flex-shrink-0 ms-2">

                                        <Nav tabs className="nav justify-content-end nav-tabs-custom rounded card-header-tabs border-bottom-0">
                                            <NavItem>
                                                <NavLink style={{ cursor: "pointer" }} className={classnames({ active: cardHeaderTab === "1", })} onClick={() => { cardHeaderToggle("1"); }} >
                                                    Home
                                                </NavLink>
                                            </NavItem>
                                            <NavItem>
                                                <NavLink style={{ cursor: "pointer" }} className={classnames({ active: cardHeaderTab === "2", })} onClick={() => { cardHeaderToggle("2"); }} >
                                                    Profile
                                                </NavLink>
                                            </NavItem>
                                            <NavItem>
                                                <NavLink style={{ cursor: "pointer" }} className={classnames({ active: cardHeaderTab === "3", })} onClick={() => { cardHeaderToggle("3"); }} >
                                                    Messages
                                                </NavLink>
                                            </NavItem>
                                        </Nav>

                                    </div>
                                </div>
                                <CardBody>
                                    <TabContent activeTab={cardHeaderTab} className="text-muted">
                                        <TabPane tabId="1" id="home2">
                                            <p className="flex-grow-1 mb-0"><img src={avatar2} alt="" className="avatar-lg rounded float-start me-3" />
                                                Food truck fixie locavore, accusamus mcsweeney's marfa nulla
                                                single-origin coffee squid. Exercitation +1 labore velit, blog
                                                sartorial PBR leggings next level wes anderson artisan four loko
                                                farm-to-table craft beer twee. commodo enim craft beer mlkshk aliquip jean shorts ullamco ad
                                                vinyl cillum PBR. Homo nostrud organic, assumenda labore
                                                aesthetic magna delectus.commodo enim craft beer mlkshk aliquip jean shorts ullamco ad
                                                vinyl cillum PBR. Homo nostrud organic, assumenda labore
                                                aesthetic magna delectus. If you’re using multiple elements, make sure that your principal object is larger than assumenda.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="2" id="profile2">
                                            <p className="me-3 mb-0">
                                                <img src={avatar3} alt="" className="avatar-lg rounded float-end ms-3" /> Experiment and play around with the fonts that you already have in the software you’re working with reputable font websites. commodo enim craft beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR. Homo nostrud organic, assumenda labore aesthetic magna delectus.commodo enim craft beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR. Homo nostrud organic, assumenda labore aesthetic magna delectus Scale all elements of your design: text, elements, buttons, everything. Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each /.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="3" id="messages2">
                                            <p className="ms-3 mb-0">
                                                <img src={avatar5} alt="" className="avatar-lg rounded float-start me-3" />Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin. Cred vinyl keffiyeh DIY salvia PBR, beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR. Homo nostrud organic, assumenda labore aes Homo nostrud organic, assumenda labore aesthetic magna delectus Scale all elements of your design: text, elements, buttons, everything.Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each.
                                            </p>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Custom Tabs Bordered</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-tabs-custom</code> class to create custom tabs with borders.</p>
                                    <Nav tabs className="nav nav-tabs nav-tabs-custom nav-success nav-justified mb-3">
                                        <NavItem>
                                            <NavLink
                                                style={{ cursor: "pointer" }}
                                                className={classnames({
                                                    active: customActiveTab === "1",
                                                })}
                                                onClick={() => {
                                                    toggleCustom("1");
                                                }}
                                            >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink
                                                style={{ cursor: "pointer" }}
                                                className={classnames({
                                                    active: customActiveTab === "2",
                                                })}
                                                onClick={() => {
                                                    toggleCustom("2");
                                                }}
                                            >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink
                                                style={{ cursor: "pointer" }}
                                                className={classnames({
                                                    active: customActiveTab === "3",
                                                })}
                                                onClick={() => {
                                                    toggleCustom("3");
                                                }}
                                            >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink
                                                style={{ cursor: "pointer" }}
                                                className={classnames({
                                                    active: customActiveTab === "4",
                                                })}
                                                onClick={() => {
                                                    toggleCustom("4");
                                                }}
                                            >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent
                                        activeTab={customActiveTab}
                                        className="text-muted"
                                    >
                                        <TabPane tabId="1" id="home1">

                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-multiple-blank-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin. Cred vinyl keffiyeh DIY salvia PBR.
                                                    <div className="mt-2">
                                                        <Link to="#" className="btn btn-sm btn-soft-primary">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
                                                    </div>
                                                </div>
                                            </div>

                                        </TabPane>
                                        <TabPane tabId="2">

                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-multiple-blank-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    When, while the lovely valley teems with vapour around me, and the meridian sun strikes the upper surface of the impenetrable foliage of my trees, and but a few stray gleams steal into the inner sanctuary, I throw myself down among the tall grass by the trickling stream; and, as I lie close to the earth, a thousand unknown.
                                                    <div className="mt-2">
                                                        <Link to="#" className="btn btn-sm btn-soft-primary">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
                                                    </div>
                                                </div>
                                            </div>

                                        </TabPane>
                                        <TabPane tabId="3">

                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-multiple-blank-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade. Messenger bag gentrify pitchfork tattooed craft beer, iphone skateboard locavore carles etsy salvia banksy hoodie helvetica. DIY synth PBR banksy irony.
                                                    <div className="mt-2">
                                                        <Link to="#" className="btn btn-sm btn-soft-primary">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
                                                    </div>
                                                </div>
                                            </div>

                                        </TabPane>
                                        <TabPane tabId="4">

                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-multiple-blank-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    when darkness overspreads my eyes, and heaven and earth seem to dwell in my soul and absorb its power, like the form of a beloved mistress, then I often think with longing, Oh, would I could describe these conceptions, could impress upon paper all that is living so full and warm within me, that it might be the.
                                                    <div className="mt-2">
                                                        <Link to="#" className="btn btn-sm btn-soft-primary">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
                                                    </div>
                                                </div>
                                            </div>

                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Arrow Nav tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>arrow-navtabs </code>class to create arrow nav tabs.</p>
                                    <Nav pills className="nav nav-pills arrow-navtabs nav-success bg-light mb-3">
                                        <NavItem>
                                            <NavLink
                                                style={{ cursor: "pointer" }}
                                                className={classnames({
                                                    active: arrowNavTab === "1",
                                                })}
                                                onClick={() => {
                                                    arrowNavToggle("1");
                                                }}
                                            >
                                                Overview
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink
                                                style={{ cursor: "pointer" }}
                                                className={classnames({
                                                    active: arrowNavTab === "2",
                                                })}
                                                onClick={() => {
                                                    arrowNavToggle("2");
                                                }}
                                            >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink
                                                style={{ cursor: "pointer" }}
                                                className={classnames({
                                                    active: arrowNavTab === "3",
                                                })}
                                                onClick={() => {
                                                    arrowNavToggle("3");
                                                }}
                                            >
                                                Contact
                                            </NavLink>
                                        </NavItem>

                                    </Nav>

                                    <TabContent
                                        activeTab={arrowNavTab}
                                        className="text-muted"
                                    >
                                        <TabPane tabId="1" id="arrow-overview">

                                            <h6>Give your text a good structure</h6>
                                            <p className="mb-0">
                                                Contrary to popular belief, you don’t have to work endless nights and hours to create a  <Link to="#" className="text-decoration-underline"><b>Fantastic Design</b></Link> by using complicated 3D elements. Flat design is your friend. Remember that. And the great thing about flat design is that it has become more and more popular over the years, which is excellent news to the beginner and advanced designer.
                                            </p>

                                        </TabPane>
                                        <TabPane tabId="2" id="arrow-profile">

                                            <h6>Use a color palette</h6>
                                            <p className="mb-0">
                                                Opposites attract, and that’s a fact. It’s in our nature to be interested in the unusual, and that’s why using contrasting colors in <Link to="#" className="text-decoration-underline"><b>Graphic Design</b></Link> is a must. It’s eye-catching, it makes a statement, it’s impressive graphic design. Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each letter has the perfect spot of its own.
                                            </p>

                                        </TabPane>
                                        <TabPane tabId="3" id="arrow-contact">

                                            <h6>Contact</h6>
                                            <p className="mb-0">
                                                Consistency is the one thing that can take all of the different elements in your design, and tie them all together and make them work. In an awareness campaign, it is vital for people to begin put 2 and 2 together and begin to recognize your cause. Consistency piques people’s interest is that it has become more and more popular over the years, which is excellent news to the beginner and advanced <Link to="#" className="text-decoration-underline"><b>Contact Designer</b></Link>.
                                            </p>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Header Justify Tabs</h5>
                            <Card>
                                <div className="card-header align-items-xl-center d-xl-flex">
                                    <p className="text-muted flex-grow-1 mb-xl-0">Use <code>card-header-pills</code> class to create header justify tab.</p>
                                    <div className="flex-shrink-0">

                                        <Nav pills className="card-header-pills">
                                            <NavItem>
                                                <NavLink style={{ cursor: "pointer" }} className={classnames({ active: headerJustifyTab === "1", })} onClick={() => { headerJustifyToggle("1"); }} >
                                                    Developers
                                                </NavLink>
                                            </NavItem>
                                            <NavItem>
                                                <NavLink style={{ cursor: "pointer" }} className={classnames({ active: headerJustifyTab === "2", })} onClick={() => { headerJustifyToggle("2"); }} >
                                                    Designers
                                                </NavLink>
                                            </NavItem>
                                            <NavItem>
                                                <NavLink style={{ cursor: "pointer" }} className={classnames({ active: headerJustifyTab === "3", })} onClick={() => { headerJustifyToggle("3"); }} >
                                                    Managers
                                                </NavLink>
                                            </NavItem>
                                        </Nav>

                                    </div>
                                </div>
                                <CardBody>
                                    <TabContent activeTab={headerJustifyTab} className="text-muted">
                                        <TabPane tabId="1" id="developers">
                                            <Row>
                                                <Col sm={6}>
                                                    <div className="d-flex">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar8} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Dominic Charlton</h6>
                                                            <p className="mb-0">520 Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar7} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Charlie Pritchard</h6>
                                                            <p className="mb-0">45.2K Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar6} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Harvey Wells</h6>
                                                            <p className="mb-0">1025 Followers</p>
                                                        </div>
                                                    </div>
                                                </Col>
                                                <Col sm={6}>
                                                    <div className="d-flex mt-3 mt-sm-0">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar5} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Amelie Townsend</h6>
                                                            <p className="mb-0">6584 Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar4} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Emily Slater</h6>
                                                            <p className="mb-0">24.8K Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar3} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Declan Long</h6>
                                                            <p className="mb-0">1.5K Followers</p>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </TabPane>

                                        <TabPane tabId="2" id="designers">
                                            <Row>
                                                <Col sm={6}>
                                                    <div className="d-flex">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar1} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Luke Brown</h6>
                                                            <p className="mb-0">654 Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar2} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Matilda Walker</h6>
                                                            <p className="mb-0">270 Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar3} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Leo Stokes</h6>
                                                            <p className="mb-0">4120 Followers</p>
                                                        </div>
                                                    </div>
                                                </Col>
                                                <Col sm={6}>
                                                    <div className="d-flex mt-3 mt-sm-0">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar4} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Chelsea Preston</h6>
                                                            <p className="mb-0">9854 Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar5} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Jennifer Barker</h6>
                                                            <p className="mb-0">15.8K Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar6} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Callum Murray</h6>
                                                            <p className="mb-0">2K Followers</p>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </TabPane>

                                        <TabPane tabId="3" id="managers">
                                            <Row>
                                                <Col sm={6}>
                                                    <div className="d-flex">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar7} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Dominic Charlton</h6>
                                                            <p className="mb-0">784 Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar8} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Matilda Walker</h6>
                                                            <p className="mb-0">6549 Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar4} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Jennifer Barker</h6>
                                                            <p className="mb-0">2100 Followers</p>
                                                        </div>
                                                    </div>
                                                </Col>
                                                <Col sm={6}>
                                                    <div className="d-flex mt-3 mt-sm-0">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar5} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Amelie Townsend</h6>
                                                            <p className="mb-0">4565 Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar1} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Emily Slater</h6>
                                                            <p className="mb-0">8K Followers</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex mt-3">
                                                        <div className="flex-shrink-0">
                                                            <img src={avatar2} alt="" className="avatar-sm rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <h6 className="mb-1 fs-14">Declan Long</h6>
                                                            <p className="mb-0">9800 Followers</p>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Bottom Card Nav Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>card-footer-tabs</code> class to create the bottom card nav justify tab.</p>
                                    <TabContent activeTab={bottomCardTab} className="text-muted">
                                        <TabPane tabId="1" id="bottomtabs-home">
                                            <h5><i className="ri-home-3-line align-bottom me-1"></i> Home</h5>
                                            <p className="mb-2">
                                                On the note of consistency, color consistency is a MUST. If you’re not trying to create crazy contrast in your design, then a great idea would be for you to use a color palette throughout your entire design. It will subconsciously interest viewers and also is very pleasing to look at.
                                            </p>
                                            <p className="mb-0">
                                                Any bypasser will stop to see what you have to see, even if your design has nothing to do with them, for the simple fact that it is beautiful to look at.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="2" id="bottomtabs-profile">
                                            <h5><i className="ri-user-2-line align-bottom me-1"></i> Profile</h5>
                                            <p className="mb-2">
                                                Just like in the image where we talked about using multiple fonts, you can see that the background in this graphic design is blurred. Whenever you put text on top of an image, it’s important that your viewers can understand the text, and sometimes that means applying a gaussian readable.
                                            </p>
                                            <p className="mb-0">
                                                A very bad quack might jinx zippy fowls. Few quips galvanized the mock jury box. Quick brown dogs jump over the lazy fox. The jay, pig, fox, zebra, and my wolves.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="3" id="bottomtabs-messages">
                                            <h5><i className="ri-mail-line align-bottom me-1"></i> Messages</h5>
                                            <p className="mb-2">
                                                Blowzy red vixens fight for a quick jump. Joaquin Phoenix was gazed by MTV for luck. A wizard’s job is to vex chumps quickly in fog. Watch "Jeopardy! ", Alex Trebek's fun TV quiz game. Woven silk pyjamas exchanged for blue quartz. Brawny gods just flocked up to quiz and vex him.
                                            </p>
                                            <p className="mb-0">
                                                Big July earthquakes confound zany experimental vow.  My girl wove six dozen plaid jackets before she quit. Six big devils from Japan quickly forgot how to waltz.
                                            </p>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                                <div className="card-footer bg-transparent border-top">

                                    <Nav pills className="nav-justified card-footer-tabs fs-17">
                                        <NavItem id="home">
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: bottomCardTab === "1", })} onClick={() => { bottonCardToggle("1"); }} >
                                                <i className="ri-home-3-line"></i>
                                            </NavLink>
                                            <UncontrolledTooltip placement="top" target="home" > Home </UncontrolledTooltip>
                                        </NavItem>
                                        <NavItem id="profile">
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: bottomCardTab === "2", })} onClick={() => { bottonCardToggle("2"); }} >
                                                <i className="ri-user-2-line"></i>
                                            </NavLink>
                                            <UncontrolledTooltip placement="top" target="profile" > Profile </UncontrolledTooltip>
                                        </NavItem>
                                        <NavItem id="messages">
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: bottomCardTab === "3", })} onClick={() => { bottonCardToggle("3"); }} >
                                                <i className="ri-mail-line"></i>
                                            </NavLink>
                                            <UncontrolledTooltip placement="top" target="messages" > Messages </UncontrolledTooltip>
                                        </NavItem>
                                    </Nav>

                                </div>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Custom Hover Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>custom-hover-nav-tabs</code> class to create custom hover tabs.</p>
                                </CardBody>
                                <div className="border">
                                    <Nav pills className="nav nav-pills custom-hover-nav-tabs">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: customHoverTab === "1", })} onClick={() => { customHovertoggle("1"); }} >
                                                <i className="ri-user-fill nav-icon nav-tab-position"></i>
                                                <h5 className="nav-titl nav-tab-position m-0">Customer</h5>
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: customHoverTab === "2", })} onClick={() => { customHovertoggle("2"); }} >
                                                <i className="ri-file-text-line nav-icon nav-tab-position"></i>
                                                <h5 className="nav-titl nav-tab-position m-0">Description</h5>
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: customHoverTab === "3", })} onClick={() => { customHovertoggle("3"); }} >
                                                <i className="ri-star-half-line nav-icon nav-tab-position"></i>
                                                <h5 className="nav-titl nav-tab-position m-0">Reviews</h5>
                                            </NavLink>
                                        </NavItem>
                                    </Nav>
                                </div>
                                <CardBody>
                                    <TabContent activeTab={customHoverTab} className="text-muted">
                                        <TabPane tabId="1" id="custom-hover-customere">
                                            <h6>Customer Details</h6>
                                            <div className="table-responsive">
                                                <table className="table mb-0">
                                                    <thead>
                                                        <tr>
                                                            <th scope="col">#</th>
                                                            <th scope="col">Name</th>
                                                            <th scope="col">Address</th>
                                                            <th scope="col">Country</th>
                                                            <th scope="col">Pincode</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        <tr>
                                                            <th scope="row">1</th>
                                                            <td>Ruby Butcher</td>
                                                            <td>412 Rosewood Lane</td>
                                                            <td>New York</td>
                                                            <td>10019</td>
                                                        </tr>
                                                        <tr>
                                                            <th scope="row">2</th>
                                                            <td>Martha T Goldberg</td>
                                                            <td>2760 Clarksburg Park Road</td>
                                                            <td>Arizona</td>
                                                            <td>86038</td>
                                                        </tr>
                                                    </tbody>
                                                </table>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="custom-hover-customere">
                                            <h6>Description</h6>
                                            <div className="table-responsive">
                                                <table className="table mb-0">
                                                    <thead>
                                                        <tr>
                                                            <th scope="col">Title</th>
                                                            <th scope="col">Categories</th>
                                                            <th scope="col">Author</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        <tr>
                                                            <td>Building Web Apps With Wordpress</td>
                                                            <td>Web, Wordpress, Design</td>
                                                            <td>Lucia Victor</td>
                                                        </tr>
                                                        <tr>
                                                            <th>Information Technology</th>
                                                            <td>Management, Manager, Design</td>
                                                            <td>Arora Sumita</td>
                                                        </tr>
                                                    </tbody>
                                                </table>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="custom-hover-reviews">
                                            <h6>Customer Reviews</h6>
                                            <div className="table-responsive">
                                                <table className="table mb-0">
                                                    <thead>
                                                        <tr>
                                                            <th scope="col">Name</th>
                                                            <th scope="col">Location</th>
                                                            <th scope="col">Amount</th>
                                                            <th scope="col">Ratings</th>
                                                        </tr>
                                                    </thead>
                                                    <tbody>
                                                        <tr>
                                                            <td>Luke Brown</td>
                                                            <td>New York</td>
                                                            <td>$745</td>
                                                            <td>4.4 <i className="ri-star-s-fill ms-1 text-warning align-middle"></i></td>
                                                        </tr>
                                                        <tr>
                                                            <td>Matilda Walker</td>
                                                            <td>USA</td>
                                                            <td>$87.125</td>
                                                            <td>2.7 <i className="ri-star-s-fill ms-1 text-warning align-middle"></i></td>
                                                        </tr>
                                                    </tbody>
                                                </table>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Custom Vertical Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>custom-verti-nav-pills</code> class to create custom vertical tabs.</p>
                                    <Row>
                                        <Col lg={3}>
                                            <Nav pills className="nav nav-pills flex-column nav-pills-tab custom-verti-nav-pills text-center">
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            "mb-2": true,
                                                            active: customverticalTab === "1",
                                                        })}
                                                        onClick={() => {
                                                            customtoggleVertical("1");
                                                        }}
                                                        id="custom-v-pills-home-tab"
                                                    >
                                                        <i className="ri-home-4-line d-block fs-20 mb-1"></i>
                                                        Home
                                                    </NavLink>
                                                </NavItem>
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            "mb-2": true,
                                                            active: customverticalTab === "2",
                                                        })}
                                                        onClick={() => {
                                                            customtoggleVertical("2");
                                                        }}
                                                        id="custom-v-pills-profile-tab"
                                                    >
                                                        <i className="ri-user-2-line d-block fs-20 mb-1"></i>
                                                        Profile
                                                    </NavLink>
                                                </NavItem>
                                                <NavItem>
                                                    <NavLink
                                                        style={{ cursor: "pointer" }}
                                                        className={classnames({
                                                            "mb-2": true,
                                                            active: customverticalTab === "3",
                                                        })}
                                                        onClick={() => {
                                                            customtoggleVertical("3");
                                                        }}
                                                        id="custom-v-pills-messages-tab"
                                                    >
                                                        <i className="ri-mail-line d-block fs-20 mb-1"></i>
                                                        Messages
                                                    </NavLink>
                                                </NavItem>
                                            </Nav>
                                        </Col>
                                        <Col lg={9}>
                                            <TabContent
                                                activeTab={customverticalTab}
                                                className="text-muted mt-3 mt-lg-0"
                                            >
                                                <TabPane tabId="1" id="custom-v-pills-home">
                                                    <div className="d-flex mb-4">
                                                        <div className="flex-shrink-0">
                                                            <img src={img4} alt="" width="150" className="rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <p className="mb-0">Experiment and play around with the fonts that you already have in the software you’re working with reputable font websites. commodo enim craft beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR. Homo nostrud organic, assumenda labore aesthetic magna delectus.</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex">
                                                        <div className="flex-grow-1 me-3">
                                                            <p className="mb-0">Always want to make sure that your fonts work well together and try to limit the number of fonts you use to three or less. Experiment and play around with the fonts that you already have in the software you’re working with reputable font websites.</p>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <img src={img5} alt="" width="150" className="rounded" />
                                                        </div>
                                                    </div>
                                                </TabPane>
                                                <TabPane tabId="2" id="custom-v-pills-profile">
                                                    <div className="d-flex mb-4">
                                                        <div className="flex-shrink-0">
                                                            <img src={img3} alt="" width="150" className="rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <p className="mb-0">In some designs, you might adjust your tracking to create a certain artistic effect. It can also help you fix fonts that are poorly spaced to begin with. A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring which I enjoy with my whole heart.</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex">
                                                        <div className="flex-grow-1 me-3">
                                                            <p className="mb-0">Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your own style choices. For that very reason, I went on a quest and spoke to many different professional graphic designers.</p>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <img src={img6} alt="" width="150" className="rounded" />
                                                        </div>
                                                    </div>

                                                </TabPane>
                                                <TabPane tabId="3" id="custom-v-pills-messages">
                                                    <div className="d-flex mb-4">
                                                        <div className="flex-shrink-0">
                                                            <img src={img7} alt="" width="150" className="rounded" />
                                                        </div>
                                                        <div className="flex-grow-1 ms-3">
                                                            <p className="mb-0">Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin. Cred vinyl keffiyeh DIY salvia PBR.</p>
                                                        </div>
                                                    </div>
                                                    <div className="d-flex">
                                                        <div className="flex-grow-1 me-3">
                                                            <p className="mb-0">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. That's why it's important to think about your message, then choose a font that fits. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <img src={img8} alt="" width="150" className="rounded" />
                                                        </div>
                                                    </div>
                                                </TabPane>
                                            </TabContent>
                                        </Col>
                                    </Row>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Animation Nav</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>animation-nav</code> class to create animated tabs.</p>

                                    <Nav pills className="nav nav-pills animation-nav nav-justified gap-2 mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: animationNavTab === "1", })} onClick={() => { animationNavToggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: animationNavTab === "2", })} onClick={() => { animationNavToggle("2"); }} >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: animationNavTab === "3", })} onClick={() => { animationNavToggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: animationNavTab === "4", })} onClick={() => { animationNavToggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={animationNavTab} className="text-muted">
                                        <TabPane tabId="1" id="animation-home">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Raw denim you probably haven't heard of them jean shorts Austin.
                                                    Nesciunt tofu stumptown aliqua, retro synth master cleanse.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="animation-profile">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    In some designs, you might adjust your tracking to create a certain artistic effect. It can also help you fix fonts that are poorly spaced to begin with.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring which I enjoy with my whole heart.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="animation-messages" >
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your own style choices.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="4" id="animation-settings">
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    After gathering lots of different opinions and graphic design basics, I came up with a list of 30 graphic design tips that you can start implementing.
                                                </div>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Nav with Badge</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Example of nav tabs with badge wrapped in nav item.</p>

                                    <Nav tabs className="nav-tabs nav-justified mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: navBadgeTab === "1", })} onClick={() => { navBadgeToggle("1"); }} >
                                                Explore
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: navBadgeTab === "2", })} onClick={() => { navBadgeToggle("2"); }} >
                                                Profile <span className="badge bg-success">Done</span>
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: navBadgeTab === "3", })} onClick={() => { navBadgeToggle("3"); }} >
                                                Messages <span className="badge bg-danger rounded-circle">5</span>
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: navBadgeTab === "4", })} onClick={() => { navBadgeToggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={navBadgeTab} className="text-muted">
                                        <TabPane tabId="1" id="nav-badge-home">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Raw denim you probably haven't heard of them jean shorts Austin.
                                                    Nesciunt tofu stumptown aliqua, retro synth master cleanse.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="nav-badge-profile">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    In some designs, you might adjust your tracking to create a certain artistic effect. It can also help you fix fonts that are poorly spaced to begin with.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring which I enjoy with my whole heart.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="nav-badge-messages" >
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your own style choices.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="4" id="nav-badge-settings">
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    After gathering lots of different opinions and graphic design basics, I came up with a list of 30 graphic design tips that you can start implementing.
                                                </div>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Border Top Nav</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-border-top</code> class to create nav tabs with border at top.</p>

                                    <Nav tabs className="nav nav-tabs nav-border-top nav-border-top-primary mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: topBorderjustifyTab === "1", })} onClick={() => { topBorderJustifytoggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: topBorderjustifyTab === "2", })} onClick={() => { topBorderJustifytoggle("2"); }} >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: topBorderjustifyTab === "3", })} onClick={() => { topBorderJustifytoggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: topBorderjustifyTab === "4", })} onClick={() => { topBorderJustifytoggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={topBorderjustifyTab} className="text-muted">
                                        <TabPane tabId="1" id="nav-border-top-home">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-line text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin. Cred vinyl keffiyeh DIY salvia PBR.
                                                    <div className="mt-2">
                                                        <Link to="#" className="btn btn-link">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
                                                    </div>
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="nav-border-top-profile">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-line text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    When, while the lovely valley teems with vapour around me, and the meridian sun strikes the upper surface of the impenetrable foliage of my trees, and but a few stray gleams steal into the inner sanctuary, I throw myself down among the tall grass by the trickling stream; and, as I lie close to the earth, a thousand unknown.
                                                    <div className="mt-2">
                                                        <Link to="#" className="btn btn-link">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
                                                    </div>
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="nav-border-top-messages">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-line text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Etsy mixtape wayfarers, ethical wes anderson tofu before they sold out mcsweeney's organic lomo retro fanny pack lo-fi farm-to-table readymade. Messenger bag gentrify pitchfork tattooed craft beer, iphone skateboard locavore carles etsy salvia banksy hoodie helvetica. DIY synth PBR banksy irony.
                                                    <div className="mt-2">
                                                        <Link to="#" className="btn btn-link">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
                                                    </div>
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="4" id="nav-border-top-settings">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-line text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    when darkness overspreads my eyes, and heaven and earth seem to dwell in my soul and absorb its power, like the form of a beloved mistress, then I often think with longing, Oh, would I could describe these conceptions, could impress upon paper all that is living so full and warm within me, that it might be the.
                                                    <div className="mt-2">
                                                        <Link to="#" className="btn btn-link">Read More <i className="ri-arrow-right-line ms-1 align-middle"></i></Link>
                                                    </div>
                                                </div>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Border Top Nav Justified Tabs</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-border-top nav-justified</code> class to create nav tabs with border at top with justified tabs position.</p>

                                    <Nav tabs className="nav nav-tabs nav-justified nav-border-top nav-border-top-success mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: topBorderTab === "1", })} onClick={() => { topBordertoggle("1"); }} >
                                                <i className="ri-home-5-line align-middle me-1"></i> Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: topBorderTab === "2", })} onClick={() => { topBordertoggle("2"); }} >
                                                <i className="ri-user-line me-1 align-middle"></i> Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: topBorderTab === "3", })} onClick={() => { topBordertoggle("3"); }} >
                                                <i className="ri-question-answer-line align-middle me-1"></i>Messages
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={topBorderTab} className="text-muted">
                                        <TabPane tabId="1" id="nav-border-justified-home">
                                            <h6>Give your text a good structure</h6>
                                            <p className="mb-0">
                                                Contrary to popular belief, you don’t have to work endless nights and hours to create a  <Link to="#" className="text-decoration-underline"><b>Fantastic Design</b></Link> by using complicated 3D elements. Flat design is your friend. Remember that. And the great thing about flat design is that it has become more and more popular over the years, which is excellent news to the beginner and advanced designer.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="2" id="nav-border-justified-profile">
                                            <h6>Use a color palette</h6>
                                            <p className="mb-0">
                                                Opposites attract, and that’s a fact. It’s in our nature to be interested in the unusual, and that’s why using contrasting colors in <Link to="#" className="text-decoration-underline"><b>Graphic Design</b></Link> is a must. It’s eye-catching, it makes a statement, it’s impressive graphic design. Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each letter has the perfect spot of its own.
                                            </p>
                                        </TabPane>

                                        <TabPane tabId="3" id="nav-border-justified-messages">
                                            <h6>Message</h6>
                                            <p className="mb-0">
                                                Consistency is the one thing that can take all of the different elements in your design, and tie them all together and make them work. In an awareness campaign, it is vital for people to begin put 2 and 2 together and begin to recognize your cause. Consistency piques people’s interest is that it has become more and more popular over the years, which is excellent news to the beginner and advanced <Link to="#" className="text-decoration-underline"><b>Contact Designer</b></Link>.
                                            </p>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Outline Border Nav</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">*Use <code>nav-custom-</code> class with modifier class to color tabs.</p>

                                    <Nav pills className="nav-custom-outline nav-primary mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: outlineBorderNav === "1", })} onClick={() => { outlineBorderNavtoggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: outlineBorderNav === "2", })} onClick={() => { outlineBorderNavtoggle("2"); }} >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: outlineBorderNav === "3", })} onClick={() => { outlineBorderNavtoggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: outlineBorderNav === "4", })} onClick={() => { outlineBorderNavtoggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>
                                    <TabContent activeTab={outlineBorderNav} className="text-muted">
                                        <TabPane tabId="1" id="border-nav-home">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Raw denim you probably haven't heard of them jean shorts Austin.
                                                    Nesciunt tofu stumptown aliqua, retro synth master cleanse.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The
                                                    goal is to make your text as comfortable to read as possible.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="border-nav-profile">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    In some designs, you might adjust your tracking to create a certain artistic effect. It can also help
                                                    you fix fonts that are poorly spaced to begin with.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    A wonderful serenity has taken possession of my entire soul, like these sweet mornings of spring which I
                                                    enjoy with my whole heart.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="border-nav-messages">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to
                                                    be creative and make your own style choices.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and
                                                    asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="4" id="border-nav-settings">
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and
                                                    asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    After gathering lots of different opinions and graphic design basics, I came up with a list of 30
                                                    graphic design tips that you can start implementing.
                                                </div>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Custom Nav</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">*Use <code>nav-custom-</code> class with modifier class to color tabs.</p>

                                    <Nav pills className="nav-customs nav-danger mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: customNav === "1" })} onClick={() => { customNavtoggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: customNav === "2" })} onClick={() => { customNavtoggle("2"); }} >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: customNav === "3" })} onClick={() => { customNavtoggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: customNav === "4" })} onClick={() => { customNavtoggle("4"); }} >
                                                Settings
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={customNav} className="text-muted">
                                        <TabPane tabId="1" id="border-navs-home">
                                            <div className="tab-pane active" id="border-navs-home" role="tabpanel">
                                                <div className="d-flex">
                                                    <div className="flex-shrink-0">
                                                        <i className="ri-checkbox-circle-fill text-success"></i>
                                                    </div>
                                                    <div className="flex-grow-1 ms-2">
                                                        Raw denim you probably haven't heard of them jean shorts Austin.
                                                        Nesciunt tofu stumptown aliqua, retro synth master cleanse.
                                                    </div>
                                                </div>
                                                <div className="d-flex mt-2">
                                                    <div className="flex-shrink-0">
                                                        <i className="ri-checkbox-circle-fill text-success"></i>
                                                    </div>
                                                    <div className="flex-grow-1 ms-2">
                                                        Too much or too little spacing, as in the example below, can make things unpleasant for the
                                                        reader. The
                                                        goal is to make your text as comfortable to read as possible.
                                                    </div>
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="border-navs-profile">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    In some designs, you might adjust your tracking to create a certain artistic effect. It can
                                                    also help
                                                    you fix fonts that are poorly spaced to begin with.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    A wonderful serenity has taken possession of my entire soul, like these sweet mornings of
                                                    spring which I
                                                    enjoy with my whole heart.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="border-navs-messages">
                                            <div className="d-flex">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    Each design is a new, unique piece of art birthed into this world, and while you have the
                                                    opportunity to
                                                    be creative and make your own style choices.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic
                                                    designers and
                                                    asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="4" id="border-navs-settings">
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic
                                                    designers and
                                                    asked them what graphic design tips they live.
                                                </div>
                                            </div>
                                            <div className="d-flex mt-2">
                                                <div className="flex-shrink-0">
                                                    <i className="ri-checkbox-circle-fill text-success"></i>
                                                </div>
                                                <div className="flex-grow-1 ms-2">
                                                    After gathering lots of different opinions and graphic design basics, I came up with a list
                                                    of 30
                                                    graphic design tips that you can start implementing.
                                                </div>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <h5 className="mb-3">Colored Nav</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-custom-</code> class with modifier class to color tabs.</p>

                                    <Nav pills className="nav nav-pills nav-custom nav-custom-success mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: colorNavTab === "1", })} onClick={() => { colorNavToggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: colorNavTab === "2", })} onClick={() => { colorNavToggle("2"); }} >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: colorNavTab === "3", })} onClick={() => { colorNavToggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={colorNavTab} className="text-muted">
                                        <TabPane tabId="1" id="nav-colored-home">
                                            <div className="d-flex mb-3">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar2} alt="" className="avatar-md rounded" />
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    Food truck fixie locavore, accusamus mcsweeney's marfa nulla
                                                    single-origin coffee squid. Exercitation +1 labore velit, blog
                                                    sartorial PBR leggings next level wes anderson artisan four loko
                                                    farm-to-table craft beer twee. commodo enim craft beer mlkshk aliquip jean shorts ullamco.
                                                </div>
                                            </div>
                                            <div className="d-flex">
                                                <div className="flex-grow-1 me-3">
                                                    For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live. vinyl cillum PBR. Homo nostrud organic, assumenda labore aesthetic magna delectus.
                                                </div>
                                                <div className="flex-shrink-0">
                                                    <img src={avatar3} alt="" className="avatar-md rounded" />
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="2" id="nav-colored-profile">
                                            <div className="d-flex mb-3">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar4} alt="" className="avatar-md rounded" />
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each. next level wes anderson artisan four loko farm-to-table craft beer twee. commodo enim craft beer mlkshk aliquip jean shorts ullamco.
                                                </div>
                                            </div>
                                            <div className="d-flex">
                                                <div className="flex-grow-1 me-3">
                                                    Experiment and play around with the fonts that you already have in the software you’re working with reputable font websites. commodo enim craft beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR. Homo nostrud organic.
                                                </div>
                                                <div className="flex-shrink-0">
                                                    <img src={avatar1} alt="" className="avatar-md rounded" />
                                                </div>
                                            </div>
                                        </TabPane>

                                        <TabPane tabId="3" id="nav-colored-messages" >
                                            <div className="d-flex mb-3">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar4} alt="" className="avatar-md rounded" />
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin.
                                                </div>
                                            </div>
                                            <div className="d-flex">
                                                <div className="flex-grow-1 me-3">
                                                    Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each assumenda labore aes Homo nostrud organic, assumenda labore aesthetic magna elements, buttons, everything..
                                                </div>
                                                <div className="flex-shrink-0">
                                                    <img src={avatar1} alt="" className="avatar-md rounded" />
                                                </div>
                                            </div>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <h5 className="mb-3">Light Nav</h5>
                            <Card>
                                <CardBody>
                                    <p className="text-muted">Use <code>nav-custom-light</code> class to lighten the nav tabs area.</p>

                                    <Nav pills className="nav nav-pills nav-custom nav-custom-light mb-3">
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: lightNavTab === "1", })} onClick={() => { lightNavToggle("1"); }} >
                                                Home
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: lightNavTab === "2", })} onClick={() => { lightNavToggle("2"); }} >
                                                Profile
                                            </NavLink>
                                        </NavItem>
                                        <NavItem>
                                            <NavLink style={{ cursor: "pointer" }} className={classnames({ active: lightNavTab === "3", })} onClick={() => { lightNavToggle("3"); }} >
                                                Messages
                                            </NavLink>
                                        </NavItem>
                                    </Nav>

                                    <TabContent activeTab={lightNavTab} className="text-muted">
                                        <TabPane tabId="1" id="nav-light-home">
                                            <div className="d-flex mb-1">
                                                <div className="flex-grow-1 me-3">
                                                    Experiment and play around with the fonts that you already have in the software you’re working with reputable font websites. commodo enim craft beer mlkshk aliquip jean shorts ullamco ad vinyl cillum PBR. Homo nostrud organic, assumenda labore aesthetic magna delectus. pposites attract, and that’s a fact. It’s in our nature to be interested in the unusual, and that’s why using contrasting.
                                                </div>
                                                <div className="flex-shrink-0">
                                                    <img src={img4} alt="" width="150" className="rounded" />
                                                </div>
                                            </div>
                                            <p className="mb-0"> Food truck fixie locavore, accusamus mcsweeney's marfa nulla
                                                single-origin coffee squid. Exercitation +1 labore velit, blog
                                                sartorial PBR leggings next level wes anderson artisan four loko
                                                farm-to-table craft beer twee. commodo enim craft beer mlkshk aliquip jean shorts ullamco.</p>
                                        </TabPane>

                                        <TabPane tabId="2" id="nav-light-profile">
                                            <div className="d-flex mb-1">
                                                <div className="flex-grow-1 me-3">
                                                    Just like in the image where we talked about using multiple fonts, you can see that the background in this graphic design is blurred. Whenever you put text on top of an image, it’s important that your viewers can understand the text, and sometimes that means applying a gaussian readable Any bypasser will stop to see what you have to see, even if your design has nothing to do with them.
                                                </div>
                                                <div className="flex-shrink-0">
                                                    <img src={img5} alt="" width="150" className="rounded" />
                                                </div>
                                            </div>
                                            <p className="mb-0">It makes a statement, it’s impressive graphic design. Increase or decrease the letter spacing depending on the situation and try, try again until it looks right, and each letter has the perfect spot of its own. commodo enim craft beer mlkshk aliquip jean shorts ullamco.</p>
                                        </TabPane>

                                        <TabPane tabId="3" id="nav-colored-messages" >
                                            <div className="d-flex mb-1">
                                                <div className="flex-grow-1 me-3">
                                                    Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin On the note of consistency, color consistency is a MUST. If you’re not trying to create crazy contrast in your design.
                                                </div>
                                                <div className="flex-shrink-0">
                                                    <img src={img5} alt="" width="150" className="rounded" />
                                                </div>
                                            </div>
                                            <p className="mb-0">Big July earthquakes confound zany experimental vow. My girl wove six dozen plaid jackets before she quit. Six big devils from Japan quickly forgot how to waltz. try again until it looks right, and each assumenda labore aes Homo nostrud organic, assumenda labore aesthetic magna elements, buttons, everything.</p>
                                        </TabPane>
                                    </TabContent>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default UiTabs;
