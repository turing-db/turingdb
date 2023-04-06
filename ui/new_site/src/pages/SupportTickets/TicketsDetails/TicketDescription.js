import React from 'react';
import { Card, CardBody, Col, Label, Row } from 'reactstrap';

//SimpleBar
import SimpleBar from "simplebar-react";

//import images
import avatar8 from "../../../assets/images/users/avatar-8.jpg";
import avatar10 from "../../../assets/images/users/avatar-10.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import image4 from "../../../assets/images/small/img-4.jpg";
import image5 from "../../../assets/images/small/img-5.jpg";
import { Link } from 'react-router-dom';
import TicketCodeExample from './TicketsDetailsCode';

const TicketDescription = () => {
    return (
        <React.Fragment>
            <Col xxl={9}>
                <Card>
                    <CardBody className="p-4">
                        <h6 className="fw-semibold text-uppercase mb-3">Ticket Discripation</h6>
                        <p className="text-muted">It would also help to know what the errors are - it could be something simple like a message saying delivery is not available which could be a problem with your shipping templates. Too much or too little spacing, as in the example below, can make things unpleasant for the reader. The goal is to make your text as comfortable to read as possible. On the note of consistency, color consistency is a MUST. If you’re not trying to create crazy contrast in your design, then a great idea would be for you to use a color palette throughout your entire design. It will subconsciously interest viewers and also is very pleasing to look at. <Link to="#" className="link-secondary text-decoration-underline">Example</Link></p>
                        <h6 className="fw-semibold text-uppercase mb-3">Create an Excellent UI for a Dashboard</h6>
                        <ul className="text-muted vstack gap-2 mb-4">
                            <li>Pick a Dashboard Type</li>
                            <li>Categorize information when needed</li>
                            <li>Provide Context</li>
                            <li>On using colors</li>
                            <li>On using the right graphs</li>
                        </ul>
                        <div className="mt-4">
                            <h6 className="fw-semibold text-uppercase mb-3">Here is the code you've requsted</h6>
                            <div>
                                <pre className="language-markup rounded-2"><code><TicketCodeExample /></code></pre>
                            </div>
                        </div>
                    </CardBody>
                    <CardBody className="p-4">
                        <h5 className="card-title mb-4">Comments</h5>

                        <SimpleBar style={{ height: "300px" }} className="px-3 mx-n3">
                            <div className="d-flex mb-4">
                                <div className="flex-shrink-0">
                                    <img src={avatar8} alt="" className="avatar-xs rounded-circle" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h5 className="fs-15">Joseph Parker <small className="text-muted">20 Dec 2021 - 05:47AM</small></h5>
                                    <p className="text-muted">I am getting message from customers that when they place order always get error message .</p>
                                    <Link to="#" className="badge text-muted bg-light"><i className="mdi mdi-reply"></i> Reply</Link>
                                    <div className="d-flex mt-4">
                                        <div className="flex-shrink-0">
                                            <img src={avatar10} alt="" className="avatar-xs rounded-circle" />
                                        </div>
                                        <div className="flex-grow-1 ms-3">
                                            <h5 className="fs-15">Alexis Clarke <small className="text-muted">22 Dec 2021 - 02:32PM</small></h5>
                                            <p className="text-muted">Please be sure to check your Spam mailbox to see if your email filters have identified the email from Dell as spam.</p>
                                            <Link to="#" className="badge text-muted bg-light"><i className="mdi mdi-reply"></i> Reply</Link>
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div className="d-flex mb-4">
                                <div className="flex-shrink-0">
                                    <img src={avatar6} alt="" className="avatar-xs rounded-circle" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h5 className="fs-15">Donald Palmer <small className="text-muted">24 Dec 2021 - 05:20PM</small></h5>
                                    <p className="text-muted">If you have further questions, please contact Customer Support from the “Action Menu” on your <Link to="#" className="text-decoration-underline">Online Order Support</Link>.</p>
                                    <Link to="#" className="badge text-muted bg-light"><i className="mdi mdi-reply"></i> Reply</Link>
                                </div>
                            </div>
                            <div className="d-flex">
                                <div className="flex-shrink-0">
                                    <img src={avatar10} alt="" className="avatar-xs rounded-circle" />
                                </div>
                                <div className="flex-grow-1 ms-3">
                                    <h5 className="fs-15">Alexis Clarke <small className="text-muted">26 min ago</small></h5>
                                    <p className="text-muted">Your <Link to="#" className="text-decoration-underline">Online Order Support</Link> provides you with the most current status of your order. To help manage your order refer to the “Action Menu” to initiate return, contact Customer Support and more.</p>
                                    <Row className="g-2 mb-3">
                                        <Col lg={1} sm={2} xs={6}>
                                            <img src={image4} alt="" className="img-fluid rounded" />
                                        </Col>
                                        <Col lg={1} sm={2} xs={6}>
                                            <img src={image5} alt="" className="img-fluid rounded" />
                                        </Col>
                                    </Row>
                                    <Link to="#" className="badge text-muted bg-light"><i className="mdi mdi-reply"></i> Reply</Link>
                                    <div className="d-flex mt-4">
                                        <div className="flex-shrink-0">
                                            <img src={avatar6} alt="" className="avatar-xs rounded-circle" />
                                        </div>
                                        <div className="flex-grow-1 ms-3">
                                            <h5 className="fs-15">Donald Palmer <small className="text-muted">8 sec ago</small></h5>
                                            <p className="text-muted">Other shipping methods are available at checkout if you want your purchase delivered faster.</p>
                                            <Link to="#" className="badge text-muted bg-light"><i className="mdi mdi-reply"></i> Reply</Link>
                                        </div>
                                    </div>
                                </div>
                            </div>
                        </SimpleBar>
                        <form action="#" className="mt-3">
                            <Row className="g-3">
                                <Col lg={12}>
                                    <Label htmlFor="exampleFormControlTextarea1" className="form-label">Leave a Comments</Label>
                                    <textarea className="form-control bg-light border-light" id="exampleFormControlTextarea1" rows="3" placeholder="Enter comments"></textarea>
                                </Col>
                                <Col lg={12} className="text-end">
                                    <Link to="#" className="btn btn-success">Post Comments</Link>
                                </Col>
                            </Row>
                        </form>
                    </CardBody>

                </Card>
            </Col>
        </React.Fragment>
    );
};

export default TicketDescription;