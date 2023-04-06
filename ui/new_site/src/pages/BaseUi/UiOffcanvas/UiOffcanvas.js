import React, { useState } from 'react';
import { Button, Card, CardBody, Col, Container, Offcanvas, OffcanvasBody, OffcanvasHeader, Row, UncontrolledTooltip, } from 'reactstrap';
import { Link } from 'react-router-dom';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
import SimpleBar from 'simplebar-react';
import { DefaultOffcanvasExample, PlacementOffcanvasExample, BackdropOffcanvasExample } from './UiOffcanvasCode';

// Import Images
import avatar1 from "../../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../../assets/images/users/avatar-7.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";

import img2 from "../../../assets/images/small/img-2.jpg";
import img3 from "../../../assets/images/small/img-3.jpg";
import img4 from "../../../assets/images/small/img-4.jpg";
import img9 from "../../../assets/images/small/img-9.jpg";
import img10 from "../../../assets/images/small/img-10.jpg";
import img11 from "../../../assets/images/small/img-11.jpg";
import img12 from "../../../assets/images/small/img-12.jpg";

const UiOffcanvas = () => {
    document.title="Offcanvas | Velzon - React Admin & Dashboard Template";

    const [open, setOpen] = useState(false);
    const [isTop, setIsTop] = useState(false);
    const [isRight, setIsRight] = useState(false);
    const [isBottom, setIsBottom] = useState(false);
    const [isLeft, setIsLeft] = useState(false);
    const [isEnableScroll, setIsEnableScroll] = useState(false);
    const [isBackdrop, setIsBackdrop] = useState(false);
    const [isScrollBackDrop, setIsScrollBackDrop] = useState(false);

    const toggleDefultCanvas = () => {
        setOpen(!open);
    };

    const toggleTopCanvas = () => {
        setIsTop(!isTop);
    };
    const toggleRightCanvas = () => {
        setIsRight(!isRight);
    };
    const toggleBottomCanvas = () => {
        setIsBottom(!isBottom);
    };
    const toggleLeftCanvas = () => {
        setIsLeft(!isLeft);
    };
    const toggleEnableScroll = () => {
        setIsEnableScroll(!isEnableScroll);
    };
    const toggleBackdrop = () => {
        setIsBackdrop(!isBackdrop);
    };
    const toggleScrollBackDrop = () => {
        setIsScrollBackDrop(!isScrollBackDrop);
    };
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
                
                <Container fluid>
                    <BreadCrumb title="Offcanvas" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Default Offcanvas" />

                                <CardBody>
                                    <p className="text-muted">Use the <code>offcanvas</code> class to set a default offcanvas.</p>
                                    <div className="live-preview">
                                        <div className="hstack flex-wrap gap-2">
                                            <Link to="#" className="btn btn-secondary" onClick={toggleDefultCanvas}>
                                                Link with href
                                            </Link>
                                            <Button color="secondary" onClick={toggleDefultCanvas}>
                                                Button with data-bs-target
                                            </Button>
                                        </div>

                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <DefaultOffcanvasExample />
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
                                <PreviewCardHeader title="Placement Offcanvas" />
                                <CardBody>
                                    <p className="text-muted">Use <code>offcanvas-top</code> , <code>offcanvas-end</code>, <code>offcanvas-bottom</code>, or <code>offcanvas-start</code>, to offcanvas class to set different Offcanvas Placement.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary" onClick={toggleTopCanvas}>Toggle Top Offcanvas</Button>
                                            <Button color="secondary" onClick={toggleRightCanvas}>Toggle Right Offcanvas</Button>
                                            <Button color="success" onClick={toggleBottomCanvas}>Toggle Bottom Offcanvas</Button>
                                            <Button color="danger" onClick={toggleLeftCanvas}>Toggle Left Offcanvas</Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <PlacementOffcanvasExample />
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
                                <PreviewCardHeader title="Backdrop Offcanvas" />
                                <CardBody>
                                    <p className="text-muted">Here are the example of offcanvas with scrolling active and backdrop visible.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button className="btn btn-light" onClick={toggleEnableScroll}>Enable Body Scrolling</Button>
                                            <Button className="btn btn-light" onClick={toggleBackdrop}>Enable Backdrop (Default)</Button>
                                            <Button className="btn btn-light" onClick={toggleScrollBackDrop}>Enable Both Scrolling & Backdrop</Button>
                                        </div>

                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BackdropOffcanvasExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                    </Row>
                </Container >
            </div >

            {/* Defult Offcanvas */}
            <Offcanvas isOpen={open} toggle={toggleDefultCanvas} id="offcanvasExample">
                <OffcanvasHeader toggle={toggleDefultCanvas} id="offcanvasExampleLabel" className="border-bottom" >
                    Recent Acitivity
                </OffcanvasHeader>
                <OffcanvasBody className="p-0 overflow-hidden">
                    <SimpleBar style={{ height: "100vh" }}>
                        <div className="acitivity-timeline p-4">
                            <div className="acitivity-item d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar1} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Oliver Phillips <span className="badge bg-soft-primary text-primary align-middle">New</span></h6>
                                    <p className="text-muted mb-2">We talked about a project on linkedin.</p>
                                    <small className="mb-0 text-muted">Today</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0 avatar-xs acitivity-avatar">
                                    <div className="avatar-title bg-soft-success text-success rounded-circle">
                                        N
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Nancy Martino <span className="badge bg-soft-secondary text-secondary align-middle">In Progress</span></h6>
                                    <p className="text-muted mb-2"><i className="ri-file-text-line align-middle ms-2"></i> Create new project Buildng product</p>
                                    <div className="avatar-group mb-2">
                                        <Link to="#" className="avatar-group-item" id="Christi">
                                            <img src={avatar4} alt="" className="rounded-circle avatar-xs" />
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Christi" > Christi </UncontrolledTooltip>
                                        <UncontrolledTooltip placement="top" target="Frank" > Frank Hook </UncontrolledTooltip>
                                        <UncontrolledTooltip placement="top" target="Ruby" > Ruby </UncontrolledTooltip>
                                        <UncontrolledTooltip placement="top" target="more" > more </UncontrolledTooltip>



                                        <Link to="#" className="avatar-group-item" id="Frank">
                                            <img src={avatar3} alt="" className="rounded-circle avatar-xs" />
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Frank" > Frank Hook </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="Ruby">
                                            <div className="avatar-xs">
                                                <div className="avatar-title rounded-circle bg-light text-primary">
                                                    R
                                                </div>
                                            </div>
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Ruby" > Ruby </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="more">
                                            <div className="avatar-xs">
                                                <div className="avatar-title rounded-circle">
                                                    2+
                                                </div>
                                            </div>
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="more" > more </UncontrolledTooltip>

                                    </div>
                                    <small className="mb-0 text-muted">Yesterday</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar2} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Natasha Carey <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                    <p className="text-muted mb-2">Adding a new event with attachments</p>
                                    <Row className="border border-dashed gx-2 p-2 mb-2">
                                        <Col className="col-4">
                                            <img src={img2} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col className="col-4">
                                            <img src={img3} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col className="col-4">
                                            <img src={img4} alt="" className="img-fluid rounded" />
                                        </Col>
                                    </Row>
                                    <small className="mb-0 text-muted">25 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar6} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Bethany Johnson</h6>
                                    <p className="text-muted mb-2">added a new member to velzon dashboard</p>
                                    <small className="mb-0 text-muted">19 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <div className="avatar-xs acitivity-avatar">
                                        <div className="avatar-title rounded-circle bg-soft-danger text-danger">
                                            <i className="ri-shopping-bag-line"></i>
                                        </div>
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Your order is placed <span className="badge bg-soft-danger text-danger align-middle ms-1">Out of Delivery</span></h6>
                                    <p className="text-muted mb-2">These customers can rest assured their order has been placed.</p>
                                    <small className="mb-0 text-muted">16 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar7} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Lewis Pratt</h6>
                                    <p className="text-muted mb-2">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. </p>
                                    <small className="mb-0 text-muted">22 Oct</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <div className="avatar-xs acitivity-avatar">
                                        <div className="avatar-title rounded-circle bg-soft-info text-info">
                                            <i className="ri-line-chart-line"></i>
                                        </div>
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Monthly sales report</h6>
                                    <p className="text-muted mb-2"><span className="text-danger">2 days left</span> notification to submit the monthly sales report. <Link to="#" className="link-warning text-decoration-underline">Reports Builder</Link></p>
                                    <small className="mb-0 text-muted">15 Oct</small>
                                </div>
                            </div>
                            <div className="acitivity-item d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar8} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">New ticket received <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                    <p className="text-muted mb-2">User <span className="text-secondary">Erica245</span> submitted a ticket.</p>
                                    <small className="mb-0 text-muted">26 Aug</small>
                                </div>
                            </div>
                        </div>
                    </SimpleBar>
                </OffcanvasBody>
                <div className="offcanvas-foorter border-top p-3 text-center">
                    <Link to="#" className="link-success">View All Acitivity <i className="ri-arrow-right-s-line align-middle ms-1"></i></Link>
                </div>
            </Offcanvas>

            {/* Top offcanvas */}
            <Offcanvas
                isOpen={isTop}
                direction="top"
                toggle={toggleTopCanvas}
                id="offcanvasTop"
                style={{ minHeight: "46vh" }}
            >
                <OffcanvasHeader toggle={toggleTopCanvas} id="offcanvasTopLabel" className="border-bottom">
                    Gallery
                </OffcanvasHeader>
                <OffcanvasBody>
                    <Row className="gallery-light">
                        <Col xl={3} lg={4} sm={6}>
                            <Card className="gallery-box light mb-0">
                                <div className="gallery-container">
                                    <Link to="#" title="">
                                        <img className="gallery-img img-fluid mx-auto" src={img12} alt="" />
                                        <div className="gallery-overlay">
                                            <h5 className="overlay-caption">A mix of friends and strangers heading off to find an adventure.</h5>
                                        </div>
                                    </Link>
                                </div>
                                <div className="box-content">
                                    <div className="d-flex align-items-center mt-1">
                                        <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">Erica Kernan</Link></div>
                                        <div className="flex-shrink-0">
                                            <div className="d-flex gap-3">
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> 3.4K
                                                </button>
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> 1.3K
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={3} lg={4} sm={6}>
                            <Card className="gallery-box light mb-0">
                                <div className="gallery-container">
                                    <Link to="#" title="">
                                        <img className="gallery-img img-fluid mx-auto" src={img9} alt="" />
                                        <div className="gallery-overlay">
                                            <h5 className="overlay-caption">Dramatic clouds at the Golden Gate Bridge</h5>
                                        </div>
                                    </Link>
                                </div>
                                <div className="box-content">
                                    <div className="d-flex align-items-center mt-1">
                                        <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">Ron Mackie</Link></div>
                                        <div className="flex-shrink-0">
                                            <div className="d-flex gap-3">
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> 5.1K
                                                </button>
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> 4.7K
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={3} lg={4} sm={6}>
                            <Card className="gallery-box mb-0">
                                <div className="gallery-container">
                                    <Link to="#" title="">
                                        <img className="gallery-img img-fluid mx-auto" src={img11} alt="" />
                                        <div className="gallery-overlay">
                                            <h5 className="overlay-caption">Cycling in the countryside</h5>
                                        </div>
                                    </Link>
                                </div>

                                <div className="box-content">
                                    <div className="d-flex align-items-center mt-1">
                                        <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">Nancy Martino</Link></div>
                                        <div className="flex-shrink-0">
                                            <div className="d-flex gap-3">
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> 3.2K
                                                </button>
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> 1.1K
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={3} lg={4} sm={6} className="d-md-none d-xl-block">
                            <Card className="gallery-box mb-0">
                                <div className="gallery-container">
                                    <Link to="#" title="">
                                        <img className="gallery-img img-fluid mx-auto" src={img10} alt="" />
                                        <div className="gallery-overlay">
                                            <h5 className="overlay-caption">Fun day at the Hill Station</h5>
                                        </div>
                                    </Link>
                                </div>
                                <div className="box-content">
                                    <div className="d-flex align-items-center mt-1">
                                        <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">Henry Baird</Link></div>
                                        <div className="flex-shrink-0">
                                            <div className="d-flex gap-3">
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> 632
                                                </button>
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> 95
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                    </Row>
                </OffcanvasBody>
            </Offcanvas>

            {/* Right offcanvas */}
            <Offcanvas
                isOpen={isRight}
                direction="end"
                toggle={toggleRightCanvas}
                id="offcanvasRight"
                className="border-bottom"
            >
                <OffcanvasHeader toggle={toggleRightCanvas} id="offcanvasRightLabel">
                    Recent Acitivity
                </OffcanvasHeader>
                <OffcanvasBody className="p-0 overflow-hidden">
                    <SimpleBar style={{ height: "100vh" }}>
                        <div className="acitivity-timeline p-4">
                            <div className="acitivity-item d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar1} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Oliver Phillips <span className="badge bg-soft-primary text-primary align-middle">New</span></h6>
                                    <p className="text-muted mb-2">We talked about a project on linkedin.</p>
                                    <small className="mb-0 text-muted">Today</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0 avatar-xs acitivity-avatar">
                                    <div className="avatar-title bg-soft-success text-success rounded-circle">
                                        N
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Nancy Martino <span className="badge bg-soft-secondary text-secondary align-middle">In Progress</span></h6>
                                    <p className="text-muted mb-2"><i className="ri-file-text-line align-middle ms-2"></i> Create new project Buildng product</p>
                                    <div className="avatar-group mb-2">
                                        <Link to="#" className="avatar-group-item" id="Christi">
                                            <img src={avatar4} alt="" className="rounded-circle avatar-xs" />
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Christi" > Christi </UncontrolledTooltip>
                                        <UncontrolledTooltip placement="top" target="Frank" > Frank Hook </UncontrolledTooltip>
                                        <UncontrolledTooltip placement="top" target="Ruby" > Ruby </UncontrolledTooltip>
                                        <UncontrolledTooltip placement="top" target="more" > more </UncontrolledTooltip>
                                        <Link to="#" className="avatar-group-item" id="Frank">
                                            <img src={avatar3} alt="" className="rounded-circle avatar-xs" />
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Frank" > Frank Hook </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="Ruby">
                                            <div className="avatar-xs">
                                                <div className="avatar-title rounded-circle bg-light text-primary">
                                                    R
                                                </div>
                                            </div>
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Ruby" > Ruby </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="more">
                                            <div className="avatar-xs">
                                                <div className="avatar-title rounded-circle">
                                                    2+
                                                </div>
                                            </div>
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="more" > more </UncontrolledTooltip>

                                    </div>
                                    <small className="mb-0 text-muted">Yesterday</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar2} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Natasha Carey <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                    <p className="text-muted mb-2">Adding a new event with attachments</p>
                                    <div className="row border border-dashed gx-2 p-2 mb-2">
                                        <Col className="col-4">
                                            <img src={img2} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col className="col-4">
                                            <img src={img3} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col className="col-4">
                                            <img src={img4} alt="" className="img-fluid rounded" />
                                        </Col>
                                    </div>
                                    <small className="mb-0 text-muted">25 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar6} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Bethany Johnson</h6>
                                    <p className="text-muted mb-2">added a new member to velzon dashboard</p>
                                    <small className="mb-0 text-muted">19 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <div className="avatar-xs acitivity-avatar">
                                        <div className="avatar-title rounded-circle bg-soft-danger text-danger">
                                            <i className="ri-shopping-bag-line"></i>
                                        </div>
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Your order is placed <span className="badge bg-soft-danger text-danger align-middle ms-1">Out of Delivery</span></h6>
                                    <p className="text-muted mb-2">These customers can rest assured their order has been placed.</p>
                                    <small className="mb-0 text-muted">16 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar7} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Lewis Pratt</h6>
                                    <p className="text-muted mb-2">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. </p>
                                    <small className="mb-0 text-muted">22 Oct</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <div className="avatar-xs acitivity-avatar">
                                        <div className="avatar-title rounded-circle bg-soft-info text-info">
                                            <i className="ri-line-chart-line"></i>
                                        </div>
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Monthly sales report</h6>
                                    <p className="text-muted mb-2"><span className="text-danger">2 days left</span> notification to submit the monthly sales report. <Link to="#" className="link-warning text-decoration-underline">Reports Builder</Link></p>
                                    <small className="mb-0 text-muted">15 Oct</small>
                                </div>
                            </div>
                            <div className="acitivity-item d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar8} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">New ticket received <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                    <p className="text-muted mb-2">User <span className="text-secondary">Erica245</span> submitted a ticket.</p>
                                    <small className="mb-0 text-muted">26 Aug</small>
                                </div>
                            </div>
                        </div>
                    </SimpleBar>
                </OffcanvasBody>
                <div className="offcanvas-foorter border p-3 text-center">
                    <Link to="#" className="link-success">View All Acitivity <i className="ri-arrow-right-s-line align-middle ms-1"></i></Link>
                </div>
            </Offcanvas>

            {/* Bottom offcanvas */}
            <Offcanvas
                isOpen={isBottom}
                direction="bottom"
                toggle={toggleBottomCanvas}
                id="offcanvasBottom"
                style={{ minHeight: "46vh" }}
            >
                <OffcanvasHeader toggle={toggleBottomCanvas} id="offcanvasBottomLabel" className="border-bottom">
                    Gallery
                </OffcanvasHeader>
                <OffcanvasBody>
                    <Row className="gallery-light">
                        <Col xl={3} lg={4} sm={6}>
                            <Card className="gallery-box light mb-0">
                                <div className="gallery-container">
                                    <Link to="#" title="">
                                        <img className="gallery-img img-fluid mx-auto" src={img12} alt="" />
                                        <div className="gallery-overlay">
                                            <h5 className="overlay-caption">A mix of friends and strangers heading off to find an adventure.</h5>
                                        </div>
                                    </Link>
                                </div>
                                <div className="box-content">
                                    <div className="d-flex align-items-center mt-1">
                                        <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">Erica Kernan</Link></div>
                                        <div className="flex-shrink-0">
                                            <div className="d-flex gap-3">
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> 3.4K
                                                </button>
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> 1.3K
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={3} lg={4} sm={6}>
                            <Card className="gallery-box light mb-0">
                                <div className="gallery-container">
                                    <Link to="#" title="">
                                        <img className="gallery-img img-fluid mx-auto" src={img9} alt="" />
                                        <div className="gallery-overlay">
                                            <h5 className="overlay-caption">Dramatic clouds at the Golden Gate Bridge</h5>
                                        </div>
                                    </Link>
                                </div>
                                <div className="box-content">
                                    <div className="d-flex align-items-center mt-1">
                                        <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">Ron Mackie</Link></div>
                                        <div className="flex-shrink-0">
                                            <div className="d-flex gap-3">
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> 5.1K
                                                </button>
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> 4.7K
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={3} lg={4} sm={6}>
                            <Card className="gallery-box mb-0">
                                <div className="gallery-container">
                                    <Link to="#" title="">
                                        <img className="gallery-img img-fluid mx-auto" src={img11} alt="" />
                                        <div className="gallery-overlay">
                                            <h5 className="overlay-caption">Cycling in the countryside</h5>
                                        </div>
                                    </Link>

                                </div>

                                <div className="box-content">
                                    <div className="d-flex align-items-center mt-1">
                                        <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">Nancy Martino</Link></div>
                                        <div className="flex-shrink-0">
                                            <div className="d-flex gap-3">
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> 5.1K
                                                </button>
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> 4.7K
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={3} lg={4} sm={6} className="d-md-none d-xl-block">
                            <Card className="gallery-box mb-0">
                                <div className="gallery-container">
                                    <Link to="#" title="">
                                        <img className="gallery-img img-fluid mx-auto" src={img10} alt="" />
                                        <div className="gallery-overlay">
                                            <h5 className="overlay-caption">Fun day at the Hill Station</h5>
                                        </div>
                                    </Link>
                                </div>
                                <div className="box-content">
                                    <div className="d-flex align-items-center mt-1">
                                        <div className="flex-grow-1 text-muted">by <Link to="#" className="text-body text-truncate">Henry Baird</Link></div>
                                        <div className="flex-shrink-0">
                                            <div className="d-flex gap-3">
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-thumb-up-fill text-muted align-bottom me-1"></i> 632
                                                </button>
                                                <button type="button" className="btn btn-sm fs-12 btn-link text-body text-decoration-none px-0">
                                                    <i className="ri-question-answer-fill text-muted align-bottom me-1"></i> 95
                                                </button>
                                            </div>
                                        </div>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                    </Row>
                </OffcanvasBody>
            </Offcanvas>

            {/* Left offcanvas */}
            <Offcanvas
                isOpen={isLeft}
                direction="start"
                toggle={toggleLeftCanvas}
                id="offcanvasLeft"
            >
                <OffcanvasHeader toggle={toggleLeftCanvas} id="offcanvasLeftLabel" className="border-bottom">
                    Recent Acitivity
                </OffcanvasHeader>
                <OffcanvasBody>
                    <SimpleBar style={{ height: "100vh" }}>
                        <div className="acitivity-timeline p-4">
                            <div className="acitivity-item d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar1} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Oliver Phillips <span className="badge bg-soft-primary text-primary align-middle">New</span></h6>
                                    <p className="text-muted mb-2">We talked about a project on linkedin.</p>
                                    <small className="mb-0 text-muted">Today</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0 avatar-xs acitivity-avatar">
                                    <div className="avatar-title bg-soft-success text-success rounded-circle">
                                        N
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Nancy Martino <span className="badge bg-soft-secondary text-secondary align-middle">In Progress</span></h6>
                                    <p className="text-muted mb-2"><i className="ri-file-text-line align-middle ms-2"></i> Create new project Buildng product</p>
                                    <div className="avatar-group mb-2">
                                        <Link to="#" className="avatar-group-item" id="Christi">
                                            <img src={avatar4} alt="" className="rounded-circle avatar-xs" />
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Christi" > Christi </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="Frank">
                                            <img src={avatar3} alt="" className="rounded-circle avatar-xs" />
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Frank" > Frank Hook </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="Ruby">
                                            <div className="avatar-xs">
                                                <div className="avatar-title rounded-circle bg-light text-primary">
                                                    R
                                                </div>
                                            </div>
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Ruby" > Ruby </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="more">
                                            <div className="avatar-xs">
                                                <div className="avatar-title rounded-circle">
                                                    2+
                                                </div>
                                            </div>
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="more" > more </UncontrolledTooltip>
                                    </div>
                                    <small className="mb-0 text-muted">Yesterday</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar2} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Natasha Carey <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                    <p className="text-muted mb-2">Adding a new event with attachments</p>
                                    <div className="row border border-dashed gx-2 p-2 mb-2">
                                        <Col className="col-4">
                                            <img src={img2} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col className="col-4">
                                            <img src={img3} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col className="col-4">
                                            <img src={img4} alt="" className="img-fluid rounded" />
                                        </Col>
                                    </div>
                                    <small className="mb-0 text-muted">25 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar6} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Bethany Johnson</h6>
                                    <p className="text-muted mb-2">added a new member to velzon dashboard</p>
                                    <small className="mb-0 text-muted">19 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <div className="avatar-xs acitivity-avatar">
                                        <div className="avatar-title rounded-circle bg-soft-danger text-danger">
                                            <i className="ri-shopping-bag-line"></i>
                                        </div>
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Your order is placed <span className="badge bg-soft-danger text-danger align-middle ms-1">Out of Delivery</span></h6>
                                    <p className="text-muted mb-2">These customers can rest assured their order has been placed.</p>
                                    <small className="mb-0 text-muted">16 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar7} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Lewis Pratt</h6>
                                    <p className="text-muted mb-2">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. </p>
                                    <small className="mb-0 text-muted">22 Oct</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <div className="avatar-xs acitivity-avatar">
                                        <div className="avatar-title rounded-circle bg-soft-info text-info">
                                            <i className="ri-line-chart-line"></i>
                                        </div>
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Monthly sales report</h6>
                                    <p className="text-muted mb-2"><span className="text-danger">2 days left</span> notification to submit the monthly sales report. <Link to="#" className="link-warning text-decoration-underline">Reports Builder</Link></p>
                                    <small className="mb-0 text-muted">15 Oct</small>
                                </div>
                            </div>
                            <div className="acitivity-item d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar8} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">New ticket received <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                    <p className="text-muted mb-2">User <span className="text-secondary">Erica245</span> submitted a ticket.</p>
                                    <small className="mb-0 text-muted">26 Aug</small>
                                </div>
                            </div>
                        </div>
                    </SimpleBar>
                </OffcanvasBody>
                <div className="offcanvas-foorter border-top p-3 text-center">
                    <Link to="#" className="link-success">View All Acitivity <i className="ri-arrow-right-s-line align-middle ms-1"></i></Link>
                </div>
            </Offcanvas>

            {/* Enable body scrolling */}
            <Offcanvas
                isOpen={isEnableScroll}
                scrollable
                backdrop={false}
                toggle={toggleEnableScroll}
                id="offcanvasScrolling"
            >
                <OffcanvasHeader toggle={toggleEnableScroll}
                    id="offcanvasScrollingLabel"
                    className="border-bottom"
                >
                    Customer Activity
                </OffcanvasHeader>
                <OffcanvasBody className="p-0 overflow-hidden">
                    <div>
                        <SimpleBar style={{ height: "100vh" }}>
                            <div className="acitivity-timeline p-4">
                                <div className="acitivity-item d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar1} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Oliver Phillips <span className="badge bg-soft-primary text-primary align-middle">New</span></h6>
                                        <p className="text-muted mb-2">We talked about a project on linkedin.</p>
                                        <small className="mb-0 text-muted">Today</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0 avatar-xs acitivity-avatar">
                                        <div className="avatar-title bg-soft-success text-success rounded-circle">
                                            N
                                        </div>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Nancy Martino <span className="badge bg-soft-secondary text-secondary align-middle">In Progress</span></h6>
                                        <p className="text-muted mb-2"><i className="ri-file-text-line align-middle ms-2"></i> Create new project Buildng product</p>
                                        <div className="avatar-group mb-2">
                                            <Link to="#" className="avatar-group-item" id="Christi">
                                                <img src={avatar4} alt="" className="rounded-circle avatar-xs" />
                                            </Link>
                                            <UncontrolledTooltip placement="top" target="Christi" > Christi </UncontrolledTooltip>
                                            <Link to="#" className="avatar-group-item" id="Frank">
                                                <img src={avatar3} alt="" className="rounded-circle avatar-xs" />
                                            </Link>
                                            <UncontrolledTooltip placement="top" target="Frank" > Frank Hook </UncontrolledTooltip>

                                            <Link to="#" className="avatar-group-item" id="Ruby">
                                                <div className="avatar-xs">
                                                    <div className="avatar-title rounded-circle bg-light text-primary">
                                                        R
                                                    </div>
                                                </div>
                                            </Link>
                                            <UncontrolledTooltip placement="top" target="Ruby" > Ruby </UncontrolledTooltip>

                                            <Link to="#" className="avatar-group-item" id="more">
                                                <div className="avatar-xs">
                                                    <div className="avatar-title rounded-circle">
                                                        2+
                                                    </div>
                                                </div>
                                            </Link>
                                            <UncontrolledTooltip placement="top" target="more" > more </UncontrolledTooltip>

                                        </div>
                                        <small className="mb-0 text-muted">Yesterday</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar2} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Natasha Carey <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                        <p className="text-muted mb-2">Adding a new event with attachments</p>
                                        <div className="row border border-dashed gx-2 p-2 mb-2">
                                            <Col className="col-4">
                                                <img src={img2} alt="" className="img-fluid rounded" />
                                            </Col>
                                            <Col className="col-4">
                                                <img src={img3} alt="" className="img-fluid rounded" />
                                            </Col>
                                            <Col className="col-4">
                                                <img src={img4} alt="" className="img-fluid rounded" />
                                            </Col>
                                        </div>
                                        <small className="mb-0 text-muted">25 Nov</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar6} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Bethany Johnson</h6>
                                        <p className="text-muted mb-2">added a new member to velzon dashboard</p>
                                        <small className="mb-0 text-muted">19 Nov</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <div className="avatar-xs acitivity-avatar">
                                            <div className="avatar-title rounded-circle bg-soft-danger text-danger">
                                                <i className="ri-shopping-bag-line"></i>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Your order is placed <span className="badge bg-soft-danger text-danger align-middle ms-1">Out of Delivery</span></h6>
                                        <p className="text-muted mb-2">These customers can rest assured their order has been placed.</p>
                                        <small className="mb-0 text-muted">16 Nov</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar7} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Lewis Pratt</h6>
                                        <p className="text-muted mb-2">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. </p>
                                        <small className="mb-0 text-muted">22 Oct</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <div className="avatar-xs acitivity-avatar">
                                            <div className="avatar-title rounded-circle bg-soft-info text-info">
                                                <i className="ri-line-chart-line"></i>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Monthly sales report</h6>
                                        <p className="text-muted mb-2"><span className="text-danger">2 days left</span> notification to submit the monthly sales report. <Link to="#" className="link-warning text-decoration-underline">Reports Builder</Link></p>
                                        <small className="mb-0 text-muted">15 Oct</small>
                                    </div>
                                </div>
                                <div className="acitivity-item d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar8} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">New ticket received <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                        <p className="text-muted mb-2">User <span className="text-secondary">Erica245</span> submitted a ticket.</p>
                                        <small className="mb-0 text-muted">26 Aug</small>
                                    </div>
                                </div>
                            </div>
                        </SimpleBar>
                    </div>
                </OffcanvasBody>
                <div className="offcanvas-foorter border-top p-3 text-center">
                    <Link to="#" className="link-success">View All Acitivity <i className="ri-arrow-right-s-line align-middle ms-1"></i></Link>
                </div>
            </Offcanvas>

            {/* Enable backdrop (default) */}
            <Offcanvas
                isOpen={isBackdrop}
                toggle={toggleBackdrop}
                id="offcanvasWithBackdrop"
                className="border-bottom"
            >
                <OffcanvasHeader toggle={toggleBackdrop} id="offcanvasWithBackdropLabel" className="border-bottom">
                    Customer Activity
                </OffcanvasHeader>
                <OffcanvasBody className="p-0 overflow-hidden">
                    <div>
                        <SimpleBar style={{ height: "100vh" }}>
                            <div className="acitivity-timeline p-4">
                                <div className="acitivity-item d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar1} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Oliver Phillips <span className="badge bg-soft-primary text-primary align-middle">New</span></h6>
                                        <p className="text-muted mb-2">We talked about a project on linkedin.</p>
                                        <small className="mb-0 text-muted">Today</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0 avatar-xs acitivity-avatar">
                                        <div className="avatar-title bg-soft-success text-success rounded-circle">
                                            N
                                        </div>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Nancy Martino <span className="badge bg-soft-secondary text-secondary align-middle">In Progress</span></h6>
                                        <p className="text-muted mb-2"><i className="ri-file-text-line align-middle ms-2"></i> Create new project Buildng product</p>
                                        <div className="avatar-group mb-2">
                                            <Link to="#" className="avatar-group-item" title="" id="Christi">
                                                <img src={avatar4} alt="" className="rounded-circle avatar-xs" />
                                            </Link>
                                            <UncontrolledTooltip placement="top" target="Christi" > Christi </UncontrolledTooltip>

                                            <Link to="#" className="avatar-group-item" title="" id="Frank">
                                                <img src={avatar3} alt="" className="rounded-circle avatar-xs" />
                                            </Link>
                                            <UncontrolledTooltip placement="top" target="Frank" > Frank Hook </UncontrolledTooltip>

                                            <Link to="#" className="avatar-group-item" title="" id="Ruby">
                                                <div className="avatar-xs">
                                                    <div className="avatar-title rounded-circle bg-light text-primary">
                                                        R
                                                    </div>
                                                </div>
                                            </Link>
                                            <UncontrolledTooltip placement="top" target="Ruby" > Ruby </UncontrolledTooltip>

                                            <Link to="#" className="avatar-group-item" title="" id="more">
                                                <div className="avatar-xs">
                                                    <div className="avatar-title rounded-circle">
                                                        2+
                                                    </div>
                                                </div>
                                            </Link>
                                            <UncontrolledTooltip placement="top" target="more" > more </UncontrolledTooltip>

                                        </div>
                                        <small className="mb-0 text-muted">Yesterday</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar2} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Natasha Carey <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                        <p className="text-muted mb-2">Adding a new event with attachments</p>
                                        <div className="row border border-dashed gx-2 p-2 mb-2">
                                            <Col className="col-4">
                                                <img src={img2} alt="" className="img-fluid rounded" />
                                            </Col>
                                            <Col className="col-4">
                                                <img src={img3} alt="" className="img-fluid rounded" />
                                            </Col>
                                            <Col className="col-4">
                                                <img src={img4} alt="" className="img-fluid rounded" />
                                            </Col>
                                        </div>
                                        <small className="mb-0 text-muted">25 Nov</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar6} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Bethany Johnson</h6>
                                        <p className="text-muted mb-2">added a new member to velzon dashboard</p>
                                        <small className="mb-0 text-muted">19 Nov</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <div className="avatar-xs acitivity-avatar">
                                            <div className="avatar-title rounded-circle bg-soft-danger text-danger">
                                                <i className="ri-shopping-bag-line"></i>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Your order is placed <span className="badge bg-soft-danger text-danger align-middle ms-1">Out of Delivery</span></h6>
                                        <p className="text-muted mb-2">These customers can rest assured their order has been placed.</p>
                                        <small className="mb-0 text-muted">16 Nov</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar7} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Lewis Pratt</h6>
                                        <p className="text-muted mb-2">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. </p>
                                        <small className="mb-0 text-muted">22 Oct</small>
                                    </div>
                                </div>
                                <div className="acitivity-item py-3 d-flex">
                                    <div className="flex-shrink-0">
                                        <div className="avatar-xs acitivity-avatar">
                                            <div className="avatar-title rounded-circle bg-soft-info text-info">
                                                <i className="ri-line-chart-line"></i>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">Monthly sales report</h6>
                                        <p className="text-muted mb-2"><span className="text-danger">2 days left</span> notification to submit the monthly sales report. <Link to="#" className="link-warning text-decoration-underline">Reports Builder</Link></p>
                                        <small className="mb-0 text-muted">15 Oct</small>
                                    </div>
                                </div>
                                <div className="acitivity-item d-flex">
                                    <div className="flex-shrink-0">
                                        <img src={avatar8} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                    </div>
                                    <div className="flex-grow-1 ms-3">
                                        <h6 className="mb-1">New ticket received <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                        <p className="text-muted mb-2">User <span className="text-secondary">Erica245</span> submitted a ticket.</p>
                                        <small className="mb-0 text-muted">26 Aug</small>
                                    </div>
                                </div>
                            </div>
                        </SimpleBar>
                    </div>
                </OffcanvasBody>
                <div className="offcanvas-foorter border-top p-3 text-center">
                    <Link to="#" className="link-success">View All Acitivity <i className="ri-arrow-right-s-line align-middle ms-1"></i></Link>
                </div>
            </Offcanvas>

            {/* Enable both scrolling & backdrop */}
            <Offcanvas
                isOpen={isScrollBackDrop}
                scrollable
                toggle={toggleScrollBackDrop}
                id="offcanvasWithBothOptions"

            >
                <OffcanvasHeader toggle={toggleScrollBackDrop} id="offcanvasWithBothOptionsLabel" className="border-bottom">
                    Customer Activity
                </OffcanvasHeader>
                <OffcanvasBody className="p-0 overflow-hidden">
                    <SimpleBar style={{ height: "100vh" }}>
                        <div className="acitivity-timeline p-4">
                            <div className="acitivity-item d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar1} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Oliver Phillips <span className="badge bg-soft-primary text-primary align-middle">New</span></h6>
                                    <p className="text-muted mb-2">We talked about a project on linkedin.</p>
                                    <small className="mb-0 text-muted">Today</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0 avatar-xs acitivity-avatar">
                                    <div className="avatar-title bg-soft-success text-success rounded-circle">
                                        N
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Nancy Martino <span className="badge bg-soft-secondary text-secondary align-middle">In Progress</span></h6>
                                    <p className="text-muted mb-2"><i className="ri-file-text-line align-middle ms-2"></i> Create new project Buildng product</p>
                                    <div className="avatar-group mb-2">
                                        <Link to="#" className="avatar-group-item" id="Christi">
                                            <img src={avatar4} alt="" className="rounded-circle avatar-xs" />
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Christi" > Christi </UncontrolledTooltip>
                                        <Link to="#" className="avatar-group-item" id="Frank">
                                            <img src={avatar3} alt="" className="rounded-circle avatar-xs" />
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Frank" > Frank Hook </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="Ruby">
                                            <div className="avatar-xs">
                                                <div className="avatar-title rounded-circle bg-light text-primary">
                                                    R
                                                </div>
                                            </div>
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="Ruby" > Ruby </UncontrolledTooltip>

                                        <Link to="#" className="avatar-group-item" id="more">
                                            <div className="avatar-xs">
                                                <div className="avatar-title rounded-circle">
                                                    2+
                                                </div>
                                            </div>
                                        </Link>
                                        <UncontrolledTooltip placement="top" target="more" > more </UncontrolledTooltip>

                                    </div>
                                    <small className="mb-0 text-muted">Yesterday</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar2} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Natasha Carey <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                    <p className="text-muted mb-2">Adding a new event with attachments</p>
                                    <div className="row border border-dashed gx-2 p-2 mb-2">
                                        <Col className="col-4">
                                            <img src={img2} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col className="col-4">
                                            <img src={img3} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col className="col-4">
                                            <img src={img4} alt="" className="img-fluid rounded" />
                                        </Col>
                                    </div>
                                    <small className="mb-0 text-muted">25 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar6} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Bethany Johnson</h6>
                                    <p className="text-muted mb-2">added a new member to velzon dashboard</p>
                                    <small className="mb-0 text-muted">19 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <div className="avatar-xs acitivity-avatar">
                                        <div className="avatar-title rounded-circle bg-soft-danger text-danger">
                                            <i className="ri-shopping-bag-line"></i>
                                        </div>
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Your order is placed <span className="badge bg-soft-danger text-danger align-middle ms-1">Out of Delivery</span></h6>
                                    <p className="text-muted mb-2">These customers can rest assured their order has been placed.</p>
                                    <small className="mb-0 text-muted">16 Nov</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar7} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Lewis Pratt</h6>
                                    <p className="text-muted mb-2">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. </p>
                                    <small className="mb-0 text-muted">22 Oct</small>
                                </div>
                            </div>
                            <div className="acitivity-item py-3 d-flex">
                                <div className="flex-shrink-0">
                                    <div className="avatar-xs acitivity-avatar">
                                        <div className="avatar-title rounded-circle bg-soft-info text-info">
                                            <i className="ri-line-chart-line"></i>
                                        </div>
                                    </div>
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">Monthly sales report</h6>
                                    <p className="text-muted mb-2"><span className="text-danger">2 days left</span> notification to submit the monthly sales report. <Link to="#" className="link-warning text-decoration-underline">Reports Builder</Link></p>
                                    <small className="mb-0 text-muted">15 Oct</small>
                                </div>
                            </div>
                            <div className="acitivity-item d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar8} alt="" className="avatar-xs rounded-circle acitivity-avatar" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h6 className="mb-1">New ticket received <span className="badge bg-soft-success text-success align-middle">Completed</span></h6>
                                    <p className="text-muted mb-2">User <span className="text-secondary">Erica245</span> submitted a ticket.</p>
                                    <small className="mb-0 text-muted">26 Aug</small>
                                </div>
                            </div>
                        </div>
                    </SimpleBar>
                </OffcanvasBody>
                <div className="offcanvas-foorter border-top p-3 text-center">
                    <Link to="#" className="link-success">View All Acitivity <i className="ri-arrow-right-s-line align-middle ms-1"></i></Link>
                </div>
            </Offcanvas>
        </React.Fragment >
    );
};

export default UiOffcanvas;
