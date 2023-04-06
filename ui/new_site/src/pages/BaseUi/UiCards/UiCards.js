import React, { useState } from 'react';
import { Card, CardBody, CardHeader, Col, Collapse, Container, Row } from 'reactstrap';
import { Link } from 'react-router-dom';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

import Masonry from 'react-masonry-css';
import "./masonry.css";

// Import Images
import img1 from "../../../assets/images/small/img-1.jpg";
import img2 from "../../../assets/images/small/img-2.jpg";
import img3 from "../../../assets/images/small/img-3.jpg";
import img4 from "../../../assets/images/small/img-4.jpg";
import img5 from "../../../assets/images/small/img-5.jpg";
import img6 from "../../../assets/images/small/img-6.jpg";
import img7 from "../../../assets/images/small/img-7.jpg";
import img8 from "../../../assets/images/small/img-8.jpg";
import img9 from "../../../assets/images/small/img-9.jpg";
import img10 from "../../../assets/images/small/img-10.jpg";
import img11 from "../../../assets/images/small/img-11.jpg";
import img12 from "../../../assets/images/small/img-12.jpg";

import avatar1 from "../../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import avatar7 from "../../../assets/images/users/avatar-7.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";

import spinner1 from "../../../assets/images/logo-sm.png";

const UiCards = () => {
    document.title = "Cards | Velzon - React Admin & Dashboard Template";

    const breakpointColumnsObj = {
        default: 3,
        1100: 3,
        700: 2,
        500: 1
    };

    const delthis = (id) => { document.getElementById(id).remove(); };

    const [coll1, setcoll1] = useState(true);
    const t_coll1 = () => {
        setcoll1(!coll1);
    };

    const [coll2, setcoll2] = useState(true);
    const t_coll2 = () => {
        setcoll2(!coll2);
    };

    const [coll3, setcoll3] = useState(true);
    const t_coll3 = () => {
        setcoll3(!coll3);
    };

    // Card Spinner
    const spinner = (id) => {
        document.getElementById(id).classList.remove("d-none");
        document.getElementById(id).classList.add("d-block");
        setTimeout(function () {
            document.getElementById(id).classList.remove("d-block");
            document.getElementById(id).classList.add("d-none");
        }, 3000);
    };

    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Cards" pageTitle="Base UI" />
                    <Row>
                        <Col sm={6} xl={3}>
                            <Card>
                                <img className="card-img-top img-fluid" src={img1} alt="Card cap" />
                                <CardBody>
                                    <h4 className="card-title mb-2">Web Developer</h4>
                                    <p className="card-text">At missed advice my it no sister. Miss told ham dull knew see she spot near can. Spirit her entire her called.</p>
                                    <div className="text-end">
                                        <Link to="#" className="btn btn-primary">Submit</Link>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col sm={6} xl={3}>
                            <Card>
                                <img className="card-img-top img-fluid" src={img2} alt="Card cap" />
                                <CardBody>
                                    <h4 className="card-title mb-2">How apps is changing the IT world</h4>
                                    <p className="card-text mb-0">Whether article spirits new her covered hastily sitting her. Money witty books nor son add. Chicken age had evening believe but proceed pretend mrs.</p>
                                </CardBody>
                                <div className="card-footer">
                                    <Link to="#" className="card-link link-secondary">Read More <i className="ri-arrow-right-s-line ms-1 align-middle lh-1"></i></Link>
                                    <Link to="#" className="card-link link-success">Bookmark <i className="ri-bookmark-line align-middle ms-1 lh-1"></i></Link>
                                </div>
                            </Card>
                        </Col>
                        <Col sm={6} xl={3}>
                            <Card>
                                <img className="card-img-top img-fluid" src={img3} alt="Card cap" />
                                <CardBody>
                                    <p className="card-text">We quickly learn to fear and thus automatically avoid potentially stressful situations of all kinds, including the most common of all making mistakes.</p>
                                </CardBody>
                                <ul className="list-group list-group-flush">
                                    <li className="list-group-item">An item</li>
                                    <li className="list-group-item">A second item</li>
                                    <li className="list-group-item">A third item</li>
                                </ul>
                            </Card>
                        </Col>
                        <Col sm={6} xl={3}>
                            <Card>
                                <CardBody>
                                    <h4 className="card-title mb-2">What planning process needs ?</h4>
                                    <h6 className="card-subtitle font-14 text-muted">Development</h6>
                                </CardBody>
                                <img className="img-fluid" src={img4} alt="Card cap" />
                                <CardBody>
                                    <p className="card-text">Objectively pursue diverse catalysts for change for interoperable meta-services. Distinctively re-engineer revolutionary meta-services.</p>
                                </CardBody>
                                <div className="card-footer">
                                    <Link to="#" className="card-link link-secondary">Read More <i className="ri-arrow-right-s-line ms-1 align-middle lh-1"></i></Link>
                                    <Link to="#" className="card-link link-success">Bookmark <i className="ri-bookmark-line align-middle ms-1 lh-1"></i></Link>
                                </div>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Using Grid Markup</h5>
                            </div>
                            <Row className="row-cols-xxl-5 row-cols-lg-3 row-cols-1">
                                <Col>
                                    <Card className="card-body">
                                        <div className="d-flex mb-4 align-items-center">
                                            <div className="flex-shrink-0">
                                                <img src={avatar1} alt="" className="avatar-sm rounded-circle" />
                                            </div>
                                            <div className="flex-grow-1 ms-2">
                                                <h5 className="card-title mb-1">Oliver Phillips</h5>
                                                <p className="text-muted mb-0">Digital Marketing</p>
                                            </div>
                                        </div>
                                        <h6 className="mb-1">$15,548</h6>
                                        <p className="card-text text-muted">Expense Account</p>
                                        <Link to="#" className="btn btn-primary btn-sm">See Details</Link>
                                    </Card>
                                </Col>
                                <Col>
                                    <Card className="card-body">
                                        <div className="d-flex mb-4 align-items-center">
                                            <div className="flex-shrink-0">
                                                <img src={avatar4} alt="" className="avatar-sm rounded-circle" />
                                            </div>
                                            <div className="flex-grow-1 ms-2">
                                                <h5 className="card-title mb-1">Natasha Carey</h5>
                                                <p className="text-muted mb-0">Manager</p>
                                            </div>
                                        </div>
                                        <h6 className="mb-1">$8,785</h6>
                                        <p className="card-text text-muted">Expense Account</p>
                                        <Link to="#" className="btn btn-primary btn-sm">See Details</Link>
                                    </Card>
                                </Col>
                                <Col>
                                    <Card className="card-body">
                                        <div className="d-flex mb-4 align-items-center">
                                            <div className="flex-shrink-0">
                                                <img src={avatar3} alt="" className="avatar-sm rounded-circle" />
                                            </div>
                                            <div className="flex-grow-1 ms-2">
                                                <h5 className="card-title mb-1">Bethany Johnson</h5>
                                                <p className="text-muted mb-0">Development</p>
                                            </div>
                                        </div>
                                        <h6 className="mb-1">$1,542</h6>
                                        <p className="card-text text-muted">Expense Account</p>
                                        <Link to="#" className="btn btn-primary btn-sm">See Details</Link>
                                    </Card>
                                </Col>
                                <Col>
                                    <Card className="card-body">
                                        <div className="d-flex mb-4 align-items-center">
                                            <div className="flex-shrink-0">
                                                <img src={avatar5} alt="" className="avatar-sm rounded-circle" />
                                            </div>
                                            <div className="flex-grow-1 ms-2">
                                                <h5 className="card-title mb-1">Erica Kernan</h5>
                                                <p className="text-muted mb-0">Fashion Designer</p>
                                            </div>
                                        </div>
                                        <h6 className="mb-1">$798</h6>
                                        <p className="card-text text-muted">Expense Account</p>
                                        <Link to="#" className="btn btn-primary btn-sm">See Details</Link>
                                    </Card>
                                </Col>
                                <Col>
                                    <Card className="card-body">
                                        <div className="d-flex mb-4 align-items-center">
                                            <div className="flex-shrink-0">
                                                <img src={avatar6} alt="" className="avatar-sm rounded-circle" />
                                            </div>
                                            <div className="flex-grow-1 ms-2">
                                                <h5 className="card-title mb-1">Lewis Pratt</h5>
                                                <p className="text-muted mb-0">Design</p>
                                            </div>
                                        </div>
                                        <h6 className="mb-1">$2,856</h6>
                                        <p className="card-text text-muted">Expense Account</p>
                                        <Link to="#" className="btn btn-primary btn-sm">See Details</Link>
                                    </Card>
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Card Text Alignment</h5>
                            </div>
                            <Row>
                                <Col lg={6} xxl={4}>
                                    <Card className="card-body">
                                        <div className="avatar-sm mb-3">
                                            <div className="avatar-title bg-soft-success text-success fs-17 rounded">
                                                <i className="ri-smartphone-line"></i>
                                            </div>
                                        </div>
                                        <h4 className="card-title">Text Application</h4>
                                        <p className="card-text text-muted">Send a link to apply on mobile device. Appropriately communicate one-to-one technology.</p>
                                        <Link to="#" className="btn btn-success">Apply Now</Link>
                                    </Card>
                                </Col>
                                <Col lg={6} xxl={4}>
                                    <div className="card card-body text-center">
                                        <div className="avatar-sm mx-auto mb-3">
                                            <div className="avatar-title bg-soft-success text-success fs-17 rounded">
                                                <i className="ri-add-line"></i>
                                            </div>
                                        </div>
                                        <h4 className="card-title">Add New Application</h4>
                                        <p className="card-text text-muted">Send a link to apply on mobile device. Appropriately communicate one-to-one technology.</p>
                                        <Link to="#" className="btn btn-success">Add New</Link>
                                    </div>
                                </Col>
                                <Col lg={6} xxl={4}>
                                    <div className="card card-body text-end">
                                        <div className="avatar-sm ms-auto mb-3">
                                            <div className="avatar-title bg-soft-success text-success fs-17 rounded">
                                                <i className="ri-gift-fill"></i>
                                            </div>
                                        </div>
                                        <h4 className="card-title">Text Application</h4>
                                        <p className="card-text text-muted">Send a link to apply on mobile device. Appropriately communicate one-to-one technology.</p>
                                        <Link to="#" className="btn btn-success">Add New</Link>
                                    </div>
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Card Header and Footer</h5>
                            </div>
                            <Row>
                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <CardHeader>
                                            <button type="button" className="btn-close float-end fs-11" aria-label="Close"></button>
                                            <h6 className="card-title mb-0">Hi, Erica Kernan</h6>
                                        </CardHeader>
                                        <CardBody>
                                            <h6 className="card-title">How to get creative in your work ?</h6>
                                            <p className="card-text text-muted mb-0">A business consulting agency is involved in the planning, implementation, and education of businesses. Miss told ham dull knew see she spot near can. Spirit her entire her called.</p>
                                        </CardBody>
                                        <div className="card-footer">
                                            <Link to="#" className="link-success float-end">Read More <i className="ri-arrow-right-s-line align-middle ms-1 lh-1"></i></Link>
                                            <p className="text-muted mb-0">1 days Ago</p>
                                        </div>
                                    </Card>
                                </Col>
                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <CardHeader>
                                            <button type="button" className="btn-close float-end fs-11" aria-label="Close"></button>
                                            <h6 className="card-title mb-0">Invoice<span className="text-secondary">#45155468</span></h6>
                                        </CardHeader>
                                        <CardBody>
                                            <h6 className="card-title">What planning process needs ?</h6>
                                            <p className="card-text text-muted mb-0">Intrinsically incubate intuitive opportunities and real-time potentialities for change for interoperable meta-services itself or distract the viewer's attention from the layout.</p>
                                        </CardBody>
                                        <div className="card-footer">
                                            <Link to="#" className="link-success float-end">Payment Now <i className="ri-arrow-right-s-line align-middle ms-1 lh-1"></i></Link>
                                            <p className="text-muted mb-0">5 days Left</p>
                                        </div>
                                    </Card>
                                </Col>
                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <CardHeader>
                                            <h6 className="card-title mb-0">Creative Agency Quote</h6>
                                        </CardHeader>
                                        <CardBody>
                                            <blockquote className="card-blockquote mb-0">
                                                <p className="text-muted mb-2">There are many variations of passages of Lorem Ipsum available.</p>
                                                <figure className="mb-0">
                                                    <blockquote className="blockquote">
                                                        <p className="lead">"A business consulting agency is involved in the planning, implementation, and education of businesses."</p>
                                                    </blockquote>
                                                    <figcaption className="blockquote-footer fs-13 text-end mb-0">
                                                        Creative Agency<cite title="Source Title" className="text-primary fw-semibold"> Alice Mellor</cite>
                                                    </figcaption>
                                                </figure>
                                            </blockquote>
                                        </CardBody>
                                    </Card>
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={4}>
                            <Card>
                                <CardBody>
                                    <h4 className="card-title mb-3"><i className="ri-user-3-fill align-middle me-1 text-muted"></i> Natasha Carey</h4>
                                    <p className="text-muted">Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible.</p>
                                    <p className="text-muted mb-0">This response is important for our ability to learn from mistakes, but it alsogives rise to self-criticism.</p>
                                </CardBody>
                                <div className="card-footer text-center">
                                    <Link to="#" className="link-secondary">View All Notification (2) <i className="ri-arrow-right-s-line align-middle ms-1 lh-1"></i></Link>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <span className="float-end text-secondary">Overview</span>
                                    <h6 className="card-title">Give your text a good structure</h6>
                                    <p className="card-subtitle text-muted mb-0">Fantastic Design</p>
                                </CardHeader>
                                <CardBody>
                                    <p className="text-muted">
                                        Contrary to popular belief, you don’t have to work endless nights and hours to create a  <Link to="#" className="text-decoration-underline"><b>Fantastic Design</b></Link> by using complicated 3D elements. Flat design is your friend. Remember that. And the great thing about flat design is that it has become more and more popular over the years.
                                    </p>
                                    <p className="text-muted mb-0">In some designs, you might adjust your tracking to create a certain artistic effect.</p>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <h6 className="card-title mb-0"><i className="ri-gift-line align-middle me-1 lh-1"></i> You've made it!</h6>
                                </CardHeader>
                                <CardBody>
                                    <p className="text-muted">After gathering lots of different opinions and graphic design basics, I came up with a list of 30 graphic design tips that you can start implementing. Each design is a new, unique piece of art birthed into this world.</p>
                                    <p className="text-muted mb-0">Art party locavore wolf cliche high life echo park Austin.</p>
                                </CardBody>
                                <div className="card-footer">
                                    <div className="hstack gap-2 justify-content-end">
                                        <Link to="#" className="btn btn-link btn-sm link-success"><i className="ri-close-line align-middle lh-1"></i> Close</Link>
                                        <Link to="#" className="btn btn-primary btn-sm">Read More</Link>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <button type="button" className="btn-close float-end fs-11" aria-label="Close"></button>
                                    <h6 className="card-title mb-0">Employee Card</h6>
                                </CardHeader>
                                <CardBody className="p-4 text-center">
                                    <div className="mx-auto avatar-md mb-3">
                                        <img src={avatar8} alt="" className="img-fluid rounded-circle" />
                                    </div>
                                    <h5 className="card-title mb-1">Gabriel Palmer</h5>
                                    <p className="text-muted mb-0">Graphic Designer</p>
                                </CardBody>
                                <div className="card-footer text-center">
                                    <ul className="list-inline mb-0">
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-secondary"><i className="ri-facebook-fill"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-success"><i className="ri-whatsapp-line"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-primary"><i className="ri-linkedin-fill"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-danger"><i className="ri-slack-fill"></i></Link>
                                        </li>
                                    </ul>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <button type="button" className="btn-close float-end fs-11" aria-label="Close"></button>
                                    <h6 className="card-title mb-0">Employee Card</h6>
                                </CardHeader>
                                <CardBody className="p-4 text-center">
                                    <div className="mx-auto avatar-md mb-3">
                                        <img src={avatar5} alt="" className="img-fluid rounded-circle" />
                                    </div>
                                    <h5 className="card-title mb-1">Amelie Townsend</h5>
                                    <p className="text-muted mb-0">Project Manager</p>
                                </CardBody>
                                <div className="card-footer text-center">
                                    <ul className="list-inline mb-0">
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-secondary"><i className="ri-facebook-fill"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-success"><i className="ri-whatsapp-line"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-primary"><i className="ri-linkedin-fill"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-danger"><i className="ri-slack-fill"></i></Link>
                                        </li>
                                    </ul>
                                </div>
                            </Card>
                        </Col>
                        <Col xl={4}>
                            <Card>
                                <CardHeader>
                                    <button type="button" className="btn-close float-end fs-11" aria-label="Close"></button>
                                    <h6 className="card-title mb-0">Employee Card</h6>
                                </CardHeader>
                                <CardBody className="p-4 text-center">
                                    <div className="mx-auto avatar-md mb-3">
                                        <img src={avatar3} alt="" className="img-fluid rounded-circle" />
                                    </div>
                                    <h5 className="card-title mb-1">Jeffrey Montgomery</h5>
                                    <p className="text-muted mb-0">UI/UX Designer</p>
                                </CardBody>
                                <div className="card-footer text-center">
                                    <ul className="list-inline mb-0">
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-secondary"><i className="ri-facebook-fill"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-success"><i className="ri-whatsapp-line"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-primary"><i className="ri-linkedin-fill"></i></Link>
                                        </li>
                                        <li className="list-inline-item">
                                            <Link to="#" className="lh-1 align-middle link-danger"><i className="ri-slack-fill"></i></Link>
                                        </li>
                                    </ul>
                                </div>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={4} id="card-none1">
                            <Card>
                                <CardHeader>
                                    <div className="d-flex align-items-center">
                                        <div className="flex-grow-1">
                                            <h6 className="card-title mb-0">Card with Spinner Loader</h6>
                                        </div>
                                        <div className="flex-shrink-0">
                                            <ul className="list-inline card-toolbar-menu d-flex align-items-center mb-0">
                                                <li className="list-inline-item">
                                                    <Link to="#" className="align-middle" onClick={() => spinner('spinner1')}>
                                                        <i className="mdi mdi-refresh align-middle"></i>
                                                    </Link>
                                                </li>
                                                <li className="list-inline-item">
                                                    <Link to="#" className="align-middle minimize-card" onClick={t_coll1} role="button">
                                                        <i className="mdi mdi-plus align-middle plus" />
                                                        <i className="mdi mdi-minus align-middle minus" />
                                                    </Link>
                                                </li>
                                                <li className="list-inline-item">
                                                    <button type="button" onClick={() => delthis("card-none1")} className="btn-close fs-10 align-middle"></button>
                                                </li>
                                            </ul>
                                        </div>
                                    </div>
                                    <div className="card-preloader d-none" id='spinner1'>
                                        <div className="card-status">
                                            <div className="spinner-border text-success"><span className="visually-hidden">Loading...</span>
                                            </div>
                                        </div>
                                    </div>
                                </CardHeader>

                                <Collapse isOpen={coll1} className="card-body" id="collapseexample1">
                                    <div className="d-flex">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            Some placeholder content for the collapse component. This panel is hidden by default but revealed when the user activates.
                                        </div>
                                    </div>
                                    <div className="d-flex mt-2">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible.
                                        </div>
                                    </div>
                                    <div className="d-flex mt-2">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            This may be the most commonly encountered tip I received from the designers I spoke with. They highly encourage that you use different fonts in one design, but do not over-exaggerate and go overboard.
                                        </div>
                                    </div>

                                    <div className="card-status visually-hidden">
                                        <div className="spinner-border text-success">
                                            <span className="visually-hidden">Loading...</span>
                                        </div>
                                    </div>
                                </Collapse>
                            </Card>
                        </Col>
                        <Col xl={4} id="card-none2">
                            <Card>
                                <CardHeader>
                                    <div className="d-flex align-items-center">
                                        <div className="flex-grow-1">
                                            <h6 className="card-title mb-0">Card with Growing Spinner Loader</h6>
                                        </div>
                                        <div className="flex-shrink-0">
                                            <ul className="list-inline card-toolbar-menu d-flex align-items-center mb-0">
                                                <li className="list-inline-item">
                                                    <Link to="#" className="align-middle" onClick={() => spinner('spinner2')}>
                                                        <i className="mdi mdi-refresh align-middle"></i>
                                                    </Link>
                                                </li>
                                                <li className="list-inline-item">
                                                    <Link to="#" className="align-middle minimize-card" role="button" onClick={t_coll2}>
                                                        <i className="mdi mdi-plus align-middle plus"></i>
                                                        <i className="mdi mdi-minus align-middle minus"></i>
                                                    </Link>
                                                </li>
                                                <li className="list-inline-item">
                                                    <button type="button" onClick={() => delthis("card-none2")} className="btn-close fs-10 align-middle"></button>
                                                </li>
                                            </ul>
                                        </div>
                                    </div>
                                    <div className="card-preloader d-none" id='spinner2'>
                                        <div className="card-status">
                                            <div className="spinner-grow text-danger">
                                                <span className="visually-hidden">Loading...</span>
                                            </div>
                                        </div>
                                    </div>
                                </CardHeader>

                                <Collapse isOpen={coll2} className="card-body" id="collapseExample2">
                                    <div className="d-flex">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            Some placeholder content for the collapse component. This panel is hidden by default but revealed when the user activates.
                                        </div>
                                    </div>
                                    <div className="d-flex mt-2">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible.
                                        </div>
                                    </div>
                                    <div className="d-flex mt-2">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            This may be the most commonly encountered tip I received from the designers I spoke with. They highly encourage that you use different fonts in one design, but do not over-exaggerate and go overboard.
                                        </div>
                                    </div>
                                </Collapse>
                            </Card>
                        </Col>
                        <Col xl={4} id="card-none3">
                            <Card>
                                <CardHeader>
                                    <div className="d-flex align-items-center">
                                        <div className="flex-grow-1">
                                            <h6 className="card-title mb-0">Card with Custom Loader</h6>
                                        </div>
                                        <div className="flex-shrink-0">
                                            <ul className="list-inline card-toolbar-menu d-flex align-items-center mb-0">
                                                <li className="list-inline-item">
                                                    <Link to="#" className="align-middle" role="button" onClick={() => spinner('spinner3')}>
                                                        <i className="mdi mdi-refresh align-middle"></i>
                                                    </Link>
                                                </li>
                                                <li className="list-inline-item">
                                                    <Link to="#" className="align-middle minimize-card" role="button" onClick={t_coll3}>
                                                        <i className="mdi mdi-plus align-middle plus"></i>
                                                        <i className="mdi mdi-minus align-middle minus"></i>
                                                    </Link>
                                                </li>
                                                <li className="list-inline-item">
                                                    <button type="button" onClick={() => delthis("card-none3")} className="btn-close fs-10 align-middle"></button>
                                                </li>
                                            </ul>
                                        </div>
                                    </div>
                                    <div className="card-preloader d-none" id='spinner3'>
                                        <div className="card-status">
                                            <img src={spinner1} alt="" className="img-fluid custom-loader" />
                                        </div>
                                    </div>
                                </CardHeader>

                                <Collapse isOpen={coll3} className="card-body" id="collapseExample3">
                                    <div className="d-flex">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            Some placeholder content for the collapse component. This panel is hidden by default but revealed when the user activates.
                                        </div>
                                    </div>
                                    <div className="d-flex mt-2">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible.
                                        </div>
                                    </div>
                                    <div className="d-flex mt-2">
                                        <div className="flex-shrink-0">
                                            <i className="ri-checkbox-circle-fill text-success"></i>
                                        </div>
                                        <div className="flex-grow-1 ms-2 text-muted">
                                            This may be the most commonly encountered tip I received from the designers I spoke with. They highly encourage that you use different fonts in one design, but do not over-exaggerate and go overboard.
                                        </div>
                                    </div>
                                </Collapse>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 text-decoration-underline">Card Image Caps & Overlays</h5>
                            </div>
                            <Row>
                                <Col lg={6} xxl={4}>
                                    <Card className="card-overlay">
                                        <img className="card-img img-fluid" src={img12} alt="Card" />
                                        <div className="card-img-overlay p-0 d-flex flex-column">
                                            <CardHeader className="bg-transparent">
                                                <h4 className="card-title text-white mb-0">Design your apps in your own way</h4>
                                            </CardHeader>
                                            <CardBody>
                                                <p className="card-text text-white mb-2">Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your unpleasant for the reader. </p>
                                                <p className="card-text">
                                                    <small className="text-white">Last updated 3 mins ago</small>
                                                </p>
                                            </CardBody>
                                            <div className="card-footer bg-transparent text-center">
                                                <Link to="#" className="link-light">Read More <i className="ri-arrow-right-s-line align-middle ms-1 lh-1"></i></Link>
                                            </div>
                                        </div>
                                    </Card>
                                </Col>
                                <Col lg={6} xxl={4}>
                                    <Card className="card-overlay">
                                        <img className="card-img img-fluid" src={img1} alt="Card" />
                                        <div className="card-img-overlay p-0">
                                            <CardHeader className="bg-transparent">
                                                <h4 className="card-title text-white mb-0">Design your apps in your own way</h4>
                                            </CardHeader>
                                            <CardBody>
                                                <p className="card-text text-white mb-2">Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your unpleasant for the reader. </p>
                                                <p className="card-text">
                                                    <small className="text-white">Last updated 3 mins ago</small>
                                                </p>
                                            </CardBody>
                                        </div>
                                    </Card>
                                </Col>
                                <Col lg={6} xxl={4}>
                                    <Card className="card-overlay">
                                        <img className="card-img img-fluid" src={img5} alt="Card" />
                                        <div className="card-img-overlay p-0" style={{ top: "auto" }}>
                                            <CardBody>
                                                <p className="card-text text-white mb-2">Each design is a new, unique piece of art birthed into this world, and while you have the opportunity to be creative and make your unpleasant for the reader. </p>
                                                <p className="card-text">
                                                    <small className="text-white">Last updated 3 mins ago</small>
                                                </p>
                                            </CardBody>
                                            <div className="card-footer bg-transparent">
                                                <h4 className="card-title text-white mb-0">Design your apps in your own way</h4>
                                            </div>
                                        </div>
                                    </Card>
                                </Col>
                            </Row>

                            <Row>
                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <img className="card-img-top img-fluid" src={img6} alt="Card cap" />
                                        <CardBody>
                                            <h4 className="card-title mb-2">A day in the of a professional fashion designer</h4>
                                            <p className="card-text text-muted"> Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee. commodo enim craft beer mlkshk aliquip jean shorts ullamco.</p>
                                            <p className="card-text">
                                                Last updated 3 mins ago
                                            </p>
                                        </CardBody>
                                    </Card>
                                </Col>
                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <CardBody>
                                            <h4 className="card-title mb-2">A day in the of a professional fashion designer</h4>
                                            <p className="card-text text-muted"> Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee. commodo enim craft beer mlkshk aliquip jean shorts ullamco.</p>
                                            <p className="card-text">
                                                Last updated 3 mins ago
                                            </p>
                                        </CardBody>
                                        <img className="card-img-bottom img-fluid" src={img7} alt="Card cap" />
                                    </Card>
                                </Col>

                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <CardBody>
                                            <h4 className="card-title mb-2">A day in the of a professional fashion designer</h4>
                                            <p className="card-text text-muted"> Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee.</p>
                                        </CardBody>
                                        <img className="card-img rounded-0 img-fluid" src={img8} alt="Card cap" />
                                        <div className="card-footer">
                                            <p className="card-text mb-0">
                                                Last updated 3 mins ago
                                            </p>
                                        </div>
                                    </Card>
                                </Col>

                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <CardHeader>
                                            <h4 className="card-title mb-0">A day in the of a professional fashion designer</h4>
                                        </CardHeader>
                                        <CardBody>
                                            <p className="card-text text-muted"> Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee.</p>
                                        </CardBody>
                                        <img className="card-img rounded-0 img-fluid" src={img10} alt="Card cap" />
                                        <div className="card-footer">
                                            <p className="card-text mb-0">
                                                Last updated 3 mins ago
                                            </p>
                                        </div>
                                    </Card>
                                </Col>

                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <img className="card-img-top img-fluid" src={img11} alt="Card cap" />
                                        <CardHeader>
                                            <h4 className="card-title mb-0">A day in the of a professional fashion designer</h4>
                                        </CardHeader>
                                        <CardBody>
                                            <p className="card-text text-muted"> Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee.</p>
                                        </CardBody>
                                        <div className="card-footer">
                                            <p className="card-text mb-0">
                                                Last updated 3 mins ago
                                            </p>
                                        </div>
                                    </Card>
                                </Col>

                                <Col lg={6} xxl={4}>
                                    <Card>
                                        <CardHeader>
                                            <h4 className="card-title mb-0">A day in the of a professional fashion designer</h4>
                                        </CardHeader>
                                        <CardBody>
                                            <p className="card-text text-muted"> Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee. ommodo enim craft beer mlkshk aliquip jean shorts ullamco.</p>
                                            <p className="card-text mb-0">
                                                Last updated 3 mins ago
                                            </p>
                                        </CardBody>
                                        <img className="card-img-bottom img-fluid" src={img11} alt="Card cap" />
                                    </Card>
                                </Col>
                            </Row>

                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Horizontal Card</h5>
                            </div>
                            <Row>
                                <Col xxl={6}>
                                    <Card>
                                        <Row className="g-0">
                                            <Col md={4}>
                                                <img className="rounded-start img-fluid h-100 object-cover" src={img12} alt="Card" />
                                            </Col>
                                            <Col md={8}>
                                                <CardHeader>
                                                    <h5 className="card-title mb-0">Give your text a good structure</h5>
                                                </CardHeader>
                                                <CardBody>
                                                    <p className="card-text mb-2">For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.</p>
                                                    <p className="card-text"><small className="text-muted">Last updated 3 mins ago</small></p>
                                                </CardBody>
                                            </Col>
                                        </Row>
                                    </Card>
                                </Col>
                                <Col xxl={6}>
                                    <Card>
                                        <Row className="g-0">
                                            <Col md={8}>
                                                <CardHeader>
                                                    <h5 className="card-title mb-0">Manage white space in responsive layouts ?</h5>
                                                </CardHeader>
                                                <CardBody>
                                                    <p className="card-text mb-2">For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.</p>
                                                    <p className="card-text"><small className="text-muted">Last updated 3 mins ago</small></p>
                                                </CardBody>
                                            </Col>
                                            <div className="col-md-4">
                                                <img className="rounded-end img-fluid h-100 object-cover" src={img4} alt="Card" />
                                            </div>
                                        </Row>
                                    </Card>
                                </Col>

                                <Col xxl={6}>
                                    <Card>
                                        <Row className="g-0">
                                            <Col md={4}>
                                                <img className="rounded-start img-fluid h-100 object-cover" src={img9} alt="Card" />
                                            </Col>
                                            <Col md={8}>
                                                <CardBody>
                                                    <h5 className="card-title mb-2">Give your text a good structure</h5>
                                                    <p className="card-text text-muted mb-0">For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.</p>
                                                </CardBody>
                                                <div className="card-footer">
                                                    <p className="card-text"><small className="text-muted">Last updated 3 mins ago</small></p>
                                                </div>
                                            </Col>
                                        </Row>
                                    </Card>
                                </Col>

                                <Col xxl={6}>
                                    <Card>
                                        <Row className="g-0">
                                            <Col md={8}>
                                                <CardBody>
                                                    <h5 className="card-title mb-2">Manage white space in responsive layouts ?</h5>
                                                    <p className="card-text text-muted mb-0">For that very reason, I went on a quest and spoke to many different professional graphic designers and asked them what graphic design tips they live.</p>
                                                </CardBody>
                                                <div className="card-footer">
                                                    <p className="card-text"><small className="text-muted">Last updated 3 mins ago</small></p>
                                                </div>
                                            </Col>
                                            <Col md={4}>
                                                <img className="rounded-end img-fluid h-100 object-cover" src={img2} alt="Card" />
                                            </Col>
                                        </Row>
                                    </Card>
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Card Background Color</h5>
                            </div>
                            <Row>
                                <Col xxl={4}>
                                    <Card className="card-primary">
                                        <CardBody>
                                            <div className="d-flex align-items-center">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar1} alt="" className="avatar-sm rounded-circle" />
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <p className="card-text"><span className="fw-medium">Jeffrey Montgomery</span> (Graphic Designer) started a new conversation.</p>
                                                </div>
                                            </div>
                                        </CardBody>
                                        <div className="card-footer">
                                            <div className="text-center">
                                                <Link to="#" className="link-light">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                            </div>
                                        </div>
                                    </Card>
                                </Col>
                                <Col xxl={4}>
                                    <Card className="card-success">
                                        <CardBody>
                                            <div className="d-flex align-items-center">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar2} alt="" className="avatar-sm rounded-circle" />
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <p className="card-text"><span className="fw-medium">Charles Dickens</span> (Financial Analyst) started a new conversation.</p>
                                                </div>
                                            </div>
                                        </CardBody>
                                        <div className="card-footer">
                                            <div className="text-center">
                                                <Link to="#" className="link-light">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                            </div>
                                        </div>
                                    </Card>
                                </Col>
                                <Col xxl={4}>
                                    <Card className="card-info">
                                        <CardBody>
                                            <div className="d-flex align-items-center">
                                                <div className="flex-shrink-0">
                                                    <img src={avatar3} alt="" className="avatar-sm rounded-circle" />
                                                </div>
                                                <div className="flex-grow-1 ms-3">
                                                    <p className="card-text"><span className="fw-medium">Oliver Phillips</span> (UI/UX Designer) started a new conversation.</p>
                                                </div>
                                            </div>
                                        </CardBody>
                                        <div className="card-footer">
                                            <div className="text-center">
                                                <Link to="#" className="link-light">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                            </div>
                                        </div>
                                    </Card>
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={4}>
                            <Card className="card-warning">
                                <CardBody>
                                    <div className="d-flex align-items-center">
                                        <div className="flex-shrink-0">
                                            <img src={avatar4} alt="" className="avatar-sm rounded-circle" />
                                        </div>
                                        <div className="flex-grow-1 ms-3">
                                            <p className="card-text"><span className="fw-medium">Rebecca Swartz</span> (Graphic Designer) started a new conversation.</p>
                                        </div>
                                    </div>
                                </CardBody>
                                <div className="card-footer">
                                    <div className="text-center">
                                        <Link to="#" className="link-light">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xxl={4}>
                            <Card className="card-danger">
                                <CardBody>
                                    <div className="d-flex align-items-center">
                                        <div className="flex-shrink-0">
                                            <img src={avatar6} alt="" className="avatar-sm rounded-circle" />
                                        </div>
                                        <div className="flex-grow-1 ms-3">
                                            <p className="card-text"><span className="fw-medium">Betty Richards</span> (Back End Developer) started a new conversation.</p>
                                        </div>
                                    </div>
                                </CardBody>
                                <div className="card-footer">
                                    <div className="text-center">
                                        <Link to="#" className="link-light">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xxl={4}>
                            <Card className="card-dark">
                                <CardBody>
                                    <div className="d-flex align-items-center">
                                        <div className="flex-shrink-0">
                                            <img src={avatar5} alt="" className="avatar-sm rounded-circle" />
                                        </div>
                                        <div className="flex-grow-1 ms-3">
                                            <p className="card-text"><span className="fw-medium">Brooke Hayes</span> (Founder & CEO) started a new conversation.</p>
                                        </div>
                                    </div>
                                </CardBody>
                                <div className="card-footer">
                                    <div className="text-center">
                                        <Link to="#" className="link-light">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={4}>
                            <Card className="card-secondary">
                                <CardBody>
                                    <div className="d-flex align-items-center">
                                        <div className="flex-shrink-0">
                                            <img src={avatar7} alt="" className="avatar-sm rounded-circle" />
                                        </div>
                                        <div className="flex-grow-1 ms-3">
                                            <p className="card-text"><span className="fw-medium">Bethany Johnson</span> (Team Leader) started a new conversation.</p>
                                        </div>
                                    </div>
                                </CardBody>
                                <div className="card-footer">
                                    <div className="text-center">
                                        <Link to="#" className="link-light">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                        <Col xxl={4}>
                            <Card className="card-light">
                                <CardBody>
                                    <div className="d-flex align-items-center">
                                        <div className="flex-shrink-0">
                                            <img src={avatar8} alt="" className="avatar-sm rounded-circle" />
                                        </div>
                                        <div className="flex-grow-1 ms-3">
                                            <p className="card-text"><span className="fw-medium">Amelie Townsend</span> (UI/UX Designer) started a new conversation.</p>
                                        </div>
                                    </div>
                                </CardBody>
                                <div className="card-footer">
                                    <div className="text-center">
                                        <Link to="#" className="link-dark">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                    </div>
                                </div>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Card Border Color</h5>
                            </div>

                            <Row>
                                <Col xxl={4}>
                                    <Card className="border card-border-primary">
                                        <CardHeader>
                                            <span className="float-end">75%</span>
                                            <h6 className="card-title mb-0">Handle to Forcast <span className="badge bg-danger align-middle fs-10">Poor</span></h6>
                                        </CardHeader>
                                        <CardBody>
                                            <p className="card-text">Whether article spirits new her covered hastily sitting her. Money witty books nor son add build on the card Chicken age had evening believe but proceed pretend mrs.</p>
                                            <div className="text-end">
                                                <Link to="#" className="link-primary fw-medium">Read More <i className="ri-arrow-right-line align-middle"></i></Link>
                                            </div>
                                        </CardBody>
                                    </Card>
                                </Col>
                                <Col xxl={4}>
                                    <Card className="border card-border-success">
                                        <CardHeader>
                                            <span className="float-end">100%</span>
                                            <h6 className="card-title mb-0">Quality Forcast <span className="badge bg-success align-middle fs-10">Excellent</span></h6>
                                        </CardHeader>
                                        <CardBody>
                                            <p className="card-text">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                            <div className="text-end">
                                                <Link to="#" className="link-success fw-medium">Read More <i className="ri-arrow-right-line align-middle"></i></Link>
                                            </div>
                                        </CardBody>
                                    </Card>
                                </Col>
                                <Col xxl={4}>
                                    <Card className="border card-border-info">
                                        <CardHeader>
                                            <h6 className="card-title mb-0">Check your E-mails <span className="badge bg-info align-middle fs-10">In Process</span></h6>
                                        </CardHeader>
                                        <CardBody>
                                            <p className="card-text">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                            <div className="text-end">
                                                <Link to="#" className="link-info fw-medium">Read More <i className="ri-arrow-right-line align-middle"></i></Link>
                                            </div>
                                        </CardBody>
                                    </Card>
                                </Col>
                            </Row>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={4}>
                            <Card className="border card-border-warning">
                                <CardHeader>
                                    <h6 className="card-title mb-0">Check your Payment <span className="badge bg-warning align-middle fs-10">Pending</span></h6>
                                </CardHeader>
                                <CardBody>
                                    <p className="card-text">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                    <div className="text-end">
                                        <Link to="#" className="link-warning fw-medium">Read More <i className="ri-arrow-right-line align-middle"></i></Link>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xxl={4}>
                            <Card className="border card-border-danger">
                                <CardHeader>
                                    <h6 className="card-title mb-0">Your Ordered Cancel <span className="badge bg-danger align-middle fs-10">Cancel</span></h6>
                                </CardHeader>
                                <CardBody>
                                    <p className="card-text">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                    <div className="text-end">
                                        <Link to="#" className="link-danger fw-medium">Read More <i className="ri-arrow-right-line align-middle"></i></Link>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xxl={4}>
                            <Card className="border card-border-dark">
                                <CardHeader>
                                    <h6 className="card-title mb-0">Handle to Forcast</h6>
                                </CardHeader>
                                <CardBody>
                                    <p className="card-text">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                    <div className="text-end">
                                        <Link to="#" className="link-dark fw-medium">Read More <i className="ri-arrow-right-line align-middle"></i></Link>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={4}>
                            <Card className="border card-border-secondary">
                                <CardHeader>
                                    <h6 className="card-title mb-0">Quality Forcast </h6>
                                </CardHeader>
                                <CardBody>
                                    <p className="card-text">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                    <div className="text-end">
                                        <Link to="#" className="link-secondary fw-medium">Read More <i className="ri-arrow-right-line align-middle"></i></Link>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xxl={4}>
                            <Card className="border card-border-light">
                                <CardHeader>
                                    <h6 className="card-title mb-0">Check your E-mails</h6>
                                </CardHeader>
                                <CardBody>
                                    <p className="card-text">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                    <div className="text-end">
                                        <Link to="#" className="link-dark fw-medium">Read More <i className="ri-arrow-right-line align-middle"></i></Link>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Card Groups</h5>
                            </div>
                            <div className="card-group">
                                <Card className="mb-4">
                                    <img className="card-img-top img-fluid" src={img4} alt="Card cap" />
                                    <CardBody>
                                        <h4 className="card-title">A day in the of a professional fashion designer</h4>
                                        <p className="card-text">This is a longer card with supporting text below as
                                            a natural lead-in to additional content. This content is a little
                                            bit longer.</p>
                                    </CardBody>
                                    <div className="card-footer">
                                        <p className="card-text">
                                            <small className="text-muted">Last updated 3 mins ago</small>
                                        </p>
                                    </div>
                                </Card>
                                <Card className="mb-4">
                                    <img className="card-img-top img-fluid" src={img5} alt="Card cap" />
                                    <CardBody>
                                        <h4 className="card-title">Manage white space in responsive layouts ?</h4>
                                        <p className="card-text">This card has supporting text below as a natural
                                            lead-in to additional content.</p>
                                    </CardBody>
                                    <div className="card-footer">
                                        <p className="card-text">
                                            <small className="text-muted">Last updated 3 mins ago</small>
                                        </p>
                                    </div>
                                </Card>
                                <Card className="mb-4">
                                    <img className="card-img-top img-fluid" src={img6} alt="Card cap" />
                                    <CardBody>
                                        <h4 className="card-title">Give your text a good structure?</h4>
                                        <p className="card-text">This is a wider card with supporting text below as
                                            a natural lead-in to additional content. This card has even longer
                                            content than the first to show that equal height action.</p>
                                    </CardBody>
                                    <div className="card-footer">
                                        <p className="card-text">
                                            <small className="text-muted">Last updated 3 mins ago</small>
                                        </p>
                                    </div>
                                </Card>
                            </div>
                        </Col>
                    </Row>

                    <Row className="g-3">
                        <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                            <h5 className="mb-0 pb-1 text-decoration-underline">Grid Card Groups</h5>
                        </div>
                        <Col className="col-xxl col-6">
                            <Card className="h-100">
                                <CardHeader>
                                    <h4 className="card-title mb-0">A day in the of a professional fashion designer</h4>
                                </CardHeader>
                                <img className="img-fluid" src={img11} alt="Card cap" />
                                <CardBody>
                                    <p className="card-text">This may be the most commonly encountered tip I received from the designers I spoke with. They highly encourage that you use different fonts in one design, but do not over-exaggerate and go overboard.</p>
                                </CardBody>
                                <div className="card-footer">
                                    <p className="card-text">
                                        <small className="text-muted">Last updated 3 mins ago</small>
                                    </p>
                                </div>
                            </Card>
                        </Col>
                        <Col className="col-xxl col-6">
                            <Card className="h-100">
                                <img className="card-img-top img-fluid" src={img10} alt="Card cap" />
                                <CardBody>
                                    <h4 className="card-title">Give your text a good structure?</h4>
                                    <p className="card-text">This is a wider card with supporting text below as
                                        a natural lead-in to additional content. This card has even longer
                                        content than the first to show that equal height action.</p>
                                </CardBody>
                                <div className="card-footer">
                                    <p className="card-text">
                                        <small className="text-muted">Last updated 3 mins ago</small>
                                    </p>
                                </div>
                            </Card>
                        </Col>
                        <Col className="col-xxl col-6">
                            <Card className="h-100">
                                <img className="card-img-top img-fluid" src={img5} alt="Card cap" />
                                <CardBody>
                                    <h4 className="card-title">Manage white space in responsive layouts ?</h4>
                                    <p className="card-text">Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee. ommodo enim craft beer mlkshk aliquip jean shorts ullamco.</p>
                                </CardBody>
                                <div className="card-footer">
                                    <p className="card-text">
                                        <small className="text-muted">Last updated 3 mins ago</small>
                                    </p>
                                </div>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col sm={12}>
                            <div className="justify-content-between d-flex align-items-center mt-5 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Cards Masonry</h5>
                            </div>

                            <Row>
                                <Masonry
                                    breakpointCols={breakpointColumnsObj}
                                    className="my-masonry-grid"
                                    columnClassName="my-masonry-grid_column"
                                >
                                    <Col>
                                        <Card>
                                            <img src={img3} className="card-img-top" alt="..." />
                                            <CardBody>
                                                <h5 className="card-title mb-1">Card title that wraps to a new line</h5>
                                                <p className="card-text">This is a longer card with supporting text below as a natural lead-in to additional content. This content is a little bit longer.</p>
                                            </CardBody>
                                        </Card>
                                    </Col>
                                    <Col>
                                        <Card>
                                            <CardBody>
                                                <h4 className="card-title mb-2">A day in the of a professional fashion designer</h4>
                                                <p className="card-text text-muted"> Exercitation +1 labore velit, blog sartorial PBR leggings next level wes anderson artisan four loko farm-to-table craft beer twee.</p>
                                            </CardBody>
                                            <img className="card-img rounded-0 img-fluid" src={img8} alt="Card cap" />
                                            <div className="card-footer">
                                                <p className="card-text mb-0">
                                                    Last updated 3 mins ago
                                                </p>
                                            </div>
                                        </Card>
                                    </Col>
                                    <Col>
                                        <Card>
                                            <img src={img7} className="card-img" alt="..." />
                                        </Card>
                                    </Col>
                                    <Card>
                                        <CardHeader>
                                            <button type="button" className="btn-close float-end fs-11" aria-label="Close"></button>
                                            <h6 className="fs-15 mb-0">Hi, Erica Kernan</h6>
                                        </CardHeader>
                                        <CardBody>
                                            <h6 className="card-title">How to get creative in your work ?</h6>
                                            <p className="card-text text-muted mb-0">A business consulting agency is involved in the planning, implementation, and education of businesses. Miss told ham dull knew see she spot near can. Spirit her entire her called.</p>
                                        </CardBody>
                                        <div className="card-footer">
                                            <Link to="#" className="link-success float-end">Read More <i className="ri-arrow-right-s-line align-middle ms-1 lh-1"></i></Link>
                                            <p className="text-muted mb-0">1 days Ago</p>
                                        </div>
                                    </Card>
                                    <Card className="bg-primary text-white text-center p-3">
                                        <blockquote className="card-blockquote m-0">
                                            <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Integer posuere erat.</p>
                                            <footer className="blockquote-footer text-white font-size-12 mt-0 mb-0">
                                                Someone famous in <cite title="Source Title">Source Title</cite>
                                            </footer>
                                        </blockquote>
                                    </Card>
                                    <Card className="p-3">
                                        <figure className="mb-0">
                                            <blockquote className="blockquote">
                                                <p className="lead">"A business consulting agency is involved in the planning, implementation, and education of businesses."</p>
                                            </blockquote>
                                            <figcaption className="blockquote-footer fs-13 text-end mb-0">
                                                Creative Agency<cite title="Source Title" className="text-primary fw-semibold"> Alice Mellor</cite>
                                            </figcaption>
                                        </figure>
                                    </Card>
                                    <div></div>
                                    <div><Card>
                                        <CardBody>
                                            <h5 className="card-title mb-1">Card title</h5>
                                            <p className="card-text">This is another card with title and supporting text below. This card has some additional content to make it slightly taller overall.</p>
                                            <p className="card-text"><small className="text-muted">Last updated 3 mins ago</small></p>
                                        </CardBody>
                                    </Card></div>
                                    <div>
                                        <Card className="card-success">
                                            <CardBody>
                                                <div className="d-flex align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar2} alt="" className="avatar-sm rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-3">
                                                        <p className="card-text"><span className="fw-medium">Charles Dickens</span> (Financial Analyst) started a new conversation.</p>
                                                    </div>
                                                </div>
                                            </CardBody>
                                            <div className="card-footer">
                                                <div className="text-center">
                                                    <Link to="#" className="link-light">Connect Now <i className="ri-arrow-right-s-line align-middle lh-1"></i></Link>
                                                </div>
                                            </div>
                                        </Card>
                                    </div>
                                    <div></div>
                                    <div></div>
                                    <div><Card className="text-center">
                                        <CardBody>
                                            <h5 className="card-title mb-1">Card title</h5>
                                            <p className="card-text">This card has a regular title and short paragraphy of text below it.</p>
                                            <p className="card-text"><small className="text-muted">Last updated 3 mins ago</small></p>
                                        </CardBody>
                                    </Card></div>
                                </Masonry>
                            </Row>
                        </Col>
                    </Row>

                    <Row>
                        <Col className="col-12">
                            <div className="justify-content-between d-flex align-items-center mt-3 mb-4">
                                <h5 className="mb-0 pb-1 text-decoration-underline">Stretched Link</h5>
                            </div>
                            <Row>
                                <Col xl={4}>
                                    <Card>
                                        <img src={img3} className="card-img-top" alt="..." />
                                        <CardBody>
                                            <h5 className="card-title">Card with stretched link</h5>
                                            <p className="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
                                            <Link to="#" className="btn btn-primary stretched-link">Go somewhere</Link>
                                        </CardBody>
                                    </Card>
                                </Col>
                                <Col xl={8}>
                                    <Card>
                                        <CardBody>
                                            <div className="d-flex position-relative">
                                                <img src={avatar3} className="flex-shrink-0 me-3 avatar-xl rounded" alt="..." />
                                                <div>
                                                    <h5 className="mt-0">Custom component with stretched link</h5>
                                                    <p>This is some placeholder content for the custom component. It is intended to mimic what some real-world content would look like, and we're using it here to give the component a bit of body and size.</p>
                                                    <Link to="#" className="stretched-link">Go somewhere</Link>
                                                </div>
                                            </div>
                                        </CardBody>
                                    </Card>
                                    <Card>
                                        <CardBody>
                                            <Row className="g-0 bg-light position-relative">
                                                <Col md={5}>
                                                    <img src={img4} className="rounded-start img-fluid" alt="..." />
                                                </Col>
                                                <Col md={7} className="p-4">
                                                    <h5 className="mt-0">Columns with stretched link</h5>
                                                    <p>Another instance of placeholder content for this other custom component. It is intended to mimic what some real-world content would look like, and we're using it here to give the component a bit of body and size.</p>
                                                    <Link to="#" className="stretched-link">Go somewhere</Link>
                                                </Col>
                                            </Row>
                                        </CardBody>
                                    </Card>
                                </Col>
                            </Row>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment>
    );
};

export default UiCards;
