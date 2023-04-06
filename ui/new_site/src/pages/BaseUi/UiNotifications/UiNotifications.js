import React, { useState } from 'react';
import { Button, Card, CardBody, Col, Container, Form, Row, Toast, ToastBody, ToastHeader, } from 'reactstrap';
import { Link } from 'react-router-dom';
import { ToastContainer, toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Images
import logosm from "../../../assets/images/logo-sm.png";

import { BootstrapToastsExample, ToastPlacementExample, BorderIconExample, ToastifyExample } from './UiNotificationsCode';

const UiNotifications = () => {
    document.title="Notifications | Velzon - React Admin & Dashboard Template";

    const [toast1, setToast1] = useState(true);
    const [toast2, setToast2] = useState(true);
    const [toast3, setToast3] = useState(true);
    const [toast4, setToast4] = useState(true);
    const [toast5, setToast5] = useState(true);
    const [toast6, setToast6] = useState(true);
    const [toast7, setToast7] = useState(false);
    const [toast8, setToast8] = useState(false);
    const [toast9, setToast9] = useState(false);
    const [toast10, setToast10] = useState(false);

    const [toast11, setToast11] = useState(true);
    const [position, setPosition] = useState();

    const defaultnotify = () => toast("Welcome Back! This is a Toast Notification", { position: "top-right", hideProgressBar: true, className: 'bg-primary text-white' });
    const successnotify = () => toast("Your application was successfully sent", { position: "top-center", hideProgressBar: true, closeOnClick: false, className: 'bg-success text-white' });
    const warningnotify = () => toast("Warning ! Something went wrong try again", { position: "top-center", hideProgressBar: true, closeOnClick: false, className: 'bg-warning text-white' });
    const errornotify = () => toast("Error ! An error occurred.", { position: "top-center", hideProgressBar: true, closeOnClick: false, className: 'bg-danger text-white' });

    const topleftnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-left", hideProgressBar: true, className: 'bg-success text-white' });
    const topcenternotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-center", hideProgressBar: true, className: 'bg-success text-white' });
    const toprightnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-right", hideProgressBar: true, className: 'bg-success text-white' });
    const bottomleftnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "bottom-left", hideProgressBar: true, className: 'bg-success text-white' });
    const bottomcenternotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "bottom-center", hideProgressBar: true, className: 'bg-success text-white' });
    const bottomrightnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "bottom-right", hideProgressBar: true, className: 'bg-success text-white' });

    const offsetnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-right", hideProgressBar: true, className: 'bg-success text-white m-5' });
    const closeiconnotify = () => toast("Welcome Back ! This is a Toast Notification", { position: "top-right", hideProgressBar: true, className: 'bg-success text-white' });
    const durationnotify = () => toast("Toast Duration 5s", { position: "top-right", hideProgressBar: false, className: 'bg-success text-white' });
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">        
                <Container fluid>
                    <BreadCrumb title="Notifications" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Bootstrap Toasts" />
                                <CardBody>
                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <Col xxl={6}>
                                                <h5 className="fs-14 mb-3">Default Toast</h5>                                                
                                                <div>
                                                    <Toast isOpen={toast1}>
                                                        <ToastHeader toggle={() => setToast1(!toast1)}>
                                                            <img src={logosm} className="rounded me-2"
                                                                alt="..." height="20" />
                                                            <span className="fw-semibold me-auto">Velzon</span>
                                                            <small style={{ marginLeft: "140px" }}>06 mins ago</small>
                                                        </ToastHeader>
                                                        <ToastBody>
                                                            Hello, world! This is a toast message.
                                                        </ToastBody>
                                                    </Toast>
                                                </div>
                                                <div className="mt-4">
                                                    <h5 className="fs-14">Translucent</h5>
                                                    <p className="text-muted">Toasts are slightly translucent, too, so they
                                                        blend over whatever
                                                        they might appear over.</p>

                                                    <div className="p-3 bg-light">
                                                        <Toast isOpen={toast2}>
                                                            <ToastHeader toggle={() => setToast2(!toast2)}>
                                                                <img src={logosm} className="rounded me-2"
                                                                    alt="..." height="20" />
                                                                <span className="fw-semibold me-auto">Velzon</span>
                                                                <small style={{ marginLeft: "150px" }}>11 mins ago</small>
                                                            </ToastHeader>
                                                            <ToastBody>
                                                                Hello, world! This is a toast message.
                                                            </ToastBody>
                                                        </Toast>
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col xxl={6}>
                                                <div>
                                                    <h5 className="fs-14">Stacking</h5>
                                                    <p className="text-muted">When you have multiple toasts, we default to
                                                        vertically stacking
                                                        them in a readable manner.</p>

                                                    <div className="p-3 bg-light">
                                                        <Toast isOpen={toast3}>
                                                            <ToastHeader toggle={() => setToast3(!toast3)}>
                                                                <img src={logosm} className="rounded me-2"
                                                                    alt="..." height="20" />
                                                                <span className="fw-semibold me-auto">Velzon</span>
                                                                <small style={{ marginLeft: "165px" }}>Just now</small>
                                                            </ToastHeader>
                                                            <ToastBody>
                                                                See? Just like this.
                                                            </ToastBody>
                                                        </Toast>

                                                        <Toast isOpen={toast4}>
                                                            <ToastHeader toggle={() => setToast4(!toast4)}>
                                                                <img src={logosm} className="rounded me-2"
                                                                    alt="..." height="20" />
                                                                <span className="fw-semibold me-auto">Velzon</span>
                                                                <small style={{ marginLeft: "130px" }}>2 seconds ago</small>
                                                            </ToastHeader>
                                                            <ToastBody>
                                                                Heads up, toasts will stack automatically
                                                            </ToastBody>
                                                        </Toast>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>

                                        <div className="mt-5">
                                            <h5 className="fs-14">Placement</h5>
                                            <Row className="g-3">
                                                <Col xxl={6}>
                                                    <div>
                                                        <p className="text-muted">Place toasts with custom CSS as you need them. The top right is often used for notifications, as is the top middle. If you’re only ever going to show one toast at a time, put the positioning styles right on the <code>.toast</code>.</p>
                                                        <div className="bg-light" style={{ position: "relative", minHeight: "200px" }}>
                                                            <Toast isOpen={toast5} style={{ position: "absolute", top: "16px", right: "16px" }}>
                                                                <ToastHeader toggle={() => setToast5(!toast5)}>
                                                                    <img src={logosm} className="rounded me-2"
                                                                        alt="..." height="20" />
                                                                    <span className="fw-semibold me-auto">Velzon</span>
                                                                    <small style={{ marginLeft: "140px" }}>06 mins ago</small>
                                                                </ToastHeader>
                                                                <ToastBody>
                                                                    Hello, world! This is a toast message.
                                                                </ToastBody>
                                                            </Toast>
                                                        </div>
                                                    </div>
                                                </Col>

                                                <Col xxl={6}>
                                                    <div>
                                                        <p className="text-muted">You can also get fancy with flexbox utilities to align toasts horizontally and/or vertically.</p>
                                                        <div className="bg-light d-flex justify-content-center align-items-center"
                                                            style={{ height: "200px" }}>
                                                            <Toast isOpen={toast6}>
                                                                <ToastHeader toggle={() => setToast6(!toast6)}>
                                                                    <img src={logosm} className="rounded me-2"
                                                                        alt="..." height="20" />
                                                                    <span className="fw-semibold me-auto">Velzon</span>
                                                                    <small style={{ marginLeft: "150px" }}>11 mins ago</small>
                                                                </ToastHeader>
                                                                <ToastBody>
                                                                    Hello, world! This is a toast message.
                                                                </ToastBody>
                                                            </Toast>
                                                        </div>
                                                    </div>
                                                </Col>
                                            </Row>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BootstrapToastsExample />
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
                                <PreviewCardHeader title="Toast Placement" />
                                <CardBody>
                                    <p className="text-muted">Various example of <code>toast placement</code>.</p>
                                    <div className="live-preview">
                                        <Form>
                                            <div className="mb-3">
                                                <select className="form-select mt-2" onChange={(e) => setPosition(e.currentTarget.value)}>
                                                    <option value="" defaultValue>Select a position...</option>
                                                    <option value="top-0 start-0">Top left</option>
                                                    <option value="top-0 start-50 translate-middle-x">Top center</option>
                                                    <option value="top-0 end-0">Top right</option>
                                                    <option value="top-50 start-0 translate-middle-y">Middle left</option>
                                                    <option value="top-50 start-50 translate-middle">Middle center</option>
                                                    <option value="top-50 end-0 translate-middle-y">Middle right</option>
                                                    <option value="bottom-0 start-0">Bottom left</option>
                                                    <option value="bottom-0 start-50 translate-middle-x">Bottom center</option>
                                                    <option value="bottom-0 end-0">Bottom right</option>
                                                </select>
                                            </div>
                                        </Form>
                                        <div aria-live="polite" aria-atomic="true" className="bd-example bg-light position-relative" style={{ minHeight: "300px" }}>
                                            <div className={"toast-container position-absolute p-3 " + position} id="toastPlacement">
                                                <Toast isOpen={toast11}>
                                                    <ToastHeader toggle={() => setToast11(!toast11)}>
                                                        <img src={logosm} className="rounded me-2"
                                                            alt="..." height="20" />
                                                        <strong className="me-auto">Velzon</strong>
                                                        <small style={{ marginLeft: "150px" }}>11 mins ago</small>
                                                    </ToastHeader>
                                                    <ToastBody>
                                                        Hello, world! This is a toast message.
                                                    </ToastBody>
                                                </Toast>
                                            </div>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ToastPlacementExample />
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
                                <PreviewCardHeader title="Bordered with Icon Toast" />
                                <CardBody>
                                    <p className="text-muted">Use  <code>toast-border-</code> with below-mentioned color variation to set a toast with border and icon.</p>
                                    <div className="live-preview">

                                        <div className="position-relative">
                                            <div className="hstack flex-wrap gap-2">
                                                <Button color='primary' className="btn btn-primary" id="borderedToast1Btn" onClick={() => setToast7(!toast7)} >Primary Toast</Button>
                                                <Button className="btn btn-success" id="borderedToast2Btn" onClick={() => setToast8(!toast8)} >Success Toast</Button>
                                                <Button className="btn btn-warning" id="borderedTost3Btn" onClick={() => setToast9(!toast9)} >Warning Toast</Button>
                                                <Button className="btn btn-danger" id="borderedToast4Btn" onClick={() => setToast10(!toast10)} >Danger Toast</Button>
                                            </div>

                                            <div style={{ zIndex: "11" }}>
                                                <Toast isOpen={toast7} id="borderedToast1" className="toast-border-primary overflow-hidden mt-3">
                                                    <ToastBody>
                                                        <div className="d-flex align-items-center">
                                                            <div className="flex-shrink-0 me-2">
                                                                <i className="ri-user-smile-line align-middle"></i>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <h6 className="mb-0">Your application was successfully sent.</h6>
                                                            </div>
                                                        </div>
                                                    </ToastBody>
                                                </Toast>
                                            </div>

                                            <div style={{ zIndex: "11" }}>
                                                <Toast isOpen={toast8} id="borderedToast2" className="toast-border-success overflow-hidden mt-3">
                                                    <ToastBody>
                                                        <div className="d-flex align-items-center">
                                                            <div className="flex-shrink-0 me-2">
                                                                <i className="ri-checkbox-circle-fill align-middle"></i>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <h6 className="mb-0">Yey! Everything worked!</h6>
                                                            </div>
                                                        </div>
                                                    </ToastBody>
                                                </Toast>
                                            </div>

                                            <div style={{ zIndex: "11" }}>
                                                <Toast isOpen={toast9} id="borderedToast3" className="toast-border-warning overflow-hidden mt-3">
                                                    <ToastBody>
                                                        <div className="d-flex align-items-center">
                                                            <div className="flex-shrink-0 me-2">
                                                                <i className="ri-notification-off-line align-middle"></i>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <h6 className="mb-0">Something went wrong, try again.</h6>
                                                            </div>
                                                        </div>
                                                    </ToastBody>
                                                </Toast>
                                            </div>
                                            <div style={{ zIndex: "11" }}>
                                                <Toast isOpen={toast10} id="borderedToast4" className="toast-border-danger overflow-hidden mt-3">
                                                    <ToastBody>
                                                        <div className="d-flex align-items-center">
                                                            <div className="flex-shrink-0 me-2">
                                                                <i className="ri-alert-line align-middle"></i>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <h6 className="mb-0">Something is very wrong!  <Link to="#" className="text-decoration-underline">See detailed report.</Link></h6>
                                                            </div>
                                                        </div>
                                                    </ToastBody>
                                                </Toast>
                                            </div>

                                        </div>

                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BorderIconExample />
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
                                <PreviewCardHeader title="Toastify JS" />
                                <CardBody>                                    
                                    <div className="live-preview">
                                        <div className="hstack flex-wrap gap-2">
                                            <Button color="light" className="w-xs" onClick={defaultnotify}>Default</Button>
                                            <Button color="light" className="w-xs" onClick={successnotify}>Success</Button>
                                            <Button color="light" className="w-xs" onClick={warningnotify}>Warning</Button>
                                            <Button color="light" className="w-xs" onClick={errornotify}>Error</Button>
                                            <ToastContainer />
                                        </div>
                                        <div className="mt-4 pt-2">
                                            <h5 className="fs-14 mb-3">Display Position</h5>
                                            <div className="hstack flex-wrap gap-2">
                                                <Button color="light" className="w-xs" onClick={topleftnotify}>Top Left</Button>
                                                <Button color="light" className="w-xs" onClick={topcenternotify}>Top Center</Button>
                                                <Button color="light" className="w-xs" onClick={toprightnotify}>Top Right</Button>
                                                <Button color="light" className="w-xs" onClick={bottomleftnotify}>Bottom Left</Button>
                                                <Button color="light" className="w-xs" onClick={bottomcenternotify}>Bottom Center</Button>
                                                <Button color="light" className="w-xs" onClick={bottomrightnotify}>Bottom Right</Button>
                                            </div>
                                        </div>

                                        <Row className="mt-3">
                                            <Col lg={4}>
                                                <div className="mt-4">
                                                    <h5 className="fs-14 mb-3">Offset Position</h5>
                                                    <div className="d-flex align-items-center flex-wrap gap-2">
                                                        <Button color="light" className="w-xs" onClick={offsetnotify} >Click Me</Button>
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col lg={4}>
                                                <div className="mt-4">
                                                    <h5 className="fs-14 mb-3">Close icon Display</h5>
                                                    <div className="d-flex align-items-center flex-wrap gap-2">
                                                        <Button color="light" className="w-xs" onClick={closeiconnotify} >Click Me</Button>
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col lg={4}>
                                                <div className="mt-4">
                                                    <h5 className="fs-14 mb-3">Duration</h5>
                                                    <div className="d-flex align-items-center flex-wrap gap-2">
                                                        <Button color="light" className="w-xs" onClick={durationnotify} >Click Me</Button>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ToastifyExample />
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

export default UiNotifications;
