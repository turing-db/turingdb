import React from 'react';
import { Card, CardBody, Col, Container, Input, ListGroup, ListGroupItem, Row, } from 'reactstrap';
import { Link } from 'react-router-dom';

import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
import SimpleBar from 'simplebar-react';
// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

// Import Images
import avatar1 from "../../../assets/images/users/avatar-1.jpg";
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";

import { DefaultListExample, ActiveItemExample, DisabledItemExample, ListlinkExample, ListButtonExample, FlushListExample, HorizontalListExample, ContextualClassExample, ContextualLinkExample, ColoredListExample, CustomContentExample, ListBadgeExample, ListCheckboxExample, ListRadioExample, ListIconExample, ListNumberedExample, CustomListExample } from './UiListCode';
const UiList = () => {
    document.title="List | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="List" pageTitle="Base UI" />
                    <Row>
                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="Default List" />

                                <CardBody>
                                    <p className="text-muted">Use <code>list-group</code> class in an unordered list and <code>list-group-item</code> class to create a default list group.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem><i className="ri-bill-line align-middle me-2"></i> Send the billing agreement</ListGroupItem>
                                            <ListGroupItem><i className="ri-file-copy-2-line align-middle me-2"></i>Send over all the documentation.</ListGroupItem>
                                            <ListGroupItem><i className="ri-question-answer-line align-middle me-2"></i>Meeting with daron to review the intake form</ListGroupItem>
                                            <ListGroupItem><i className="ri-secure-payment-line align-middle me-2"></i>Check uikings theme and give customer support</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <DefaultListExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="Active Item" />

                                <CardBody>
                                    <p className="text-muted">Use <code>active</code> class to <code>list-group-item</code> to indicate the current active selection.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem active>Send the billing agreement</ListGroupItem>
                                            <ListGroupItem>Send over all the documentation.</ListGroupItem>
                                            <ListGroupItem>Meeting with daron to review the intake form</ListGroupItem>
                                            <ListGroupItem>Check uikings theme and give customer support</ListGroupItem>
                                            <ListGroupItem>Start making a presentation</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ActiveItemExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="Disabled Items" />

                                <CardBody>
                                    <p className="text-muted">Use <code>disabled</code> class to <code>list-group-item</code> to make it <em>appear</em> disabled.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem disabled>
                                                <div className="d-flex align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar1} alt="" className="avatar-xs rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-2">
                                                        James Ballard
                                                    </div>
                                                </div>
                                            </ListGroupItem>
                                            <ListGroupItem>
                                                <div className="d-flex align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar2} alt="" className="avatar-xs rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-2">
                                                        Nancy Martino
                                                    </div>
                                                </div>
                                            </ListGroupItem>
                                            <ListGroupItem>
                                                <div className="d-flex align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar3} alt="" className="avatar-xs rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-2">
                                                        Henry Baird
                                                    </div>
                                                </div>
                                            </ListGroupItem>
                                            <ListGroupItem>
                                                <div className="d-flex align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar3} alt="" className="avatar-xs rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-2">
                                                        Erica Kernan
                                                    </div>
                                                </div>
                                            </ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "215px" }}>
                                            <code>
                                                <DisabledItemExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="List with Link" />

                                <CardBody>
                                    <p className="text-muted">Use <code>tag="a"</code> to create actionable list group items with hover, disabled, and active states by adding list-group-item-action.</p>
                                    <div className="live-preview">
                                        <ListGroup className="list-group-fill-success">
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action active"><i className="ri-download-2-fill align-middle me-2"></i>Category Download</ListGroupItem>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action"><i className="ri-shield-check-line align-middle me-2"></i>Security Access</ListGroupItem>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action"><i className="ri-database-2-line align-middle me-2"></i>Storage folder</ListGroupItem>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action"><i className="ri-notification-3-line align-middle me-2"></i>Push Notification</ListGroupItem>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action disabled" tabIndex="-1"><i className="ri-moon-fill align-middle me-2"></i>Dark Mode</ListGroupItem>
                                        </ListGroup>
                                    </div>


                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ListlinkExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="List with Button" />

                                <CardBody>
                                    <p className="text-muted">Use <code>tag="button"</code> to create actionable list group items with hover, disabled, and active states by adding list-group-item-action.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem tag="button" className="list-group-item-action active"><i className="ri-reply-fill align-middle me-2"></i>Reply</ListGroupItem>
                                            <ListGroupItem tag="button" className="list-group-item-action"><i className="ri-share-forward-fill align-middle me-2"></i>Forward Message</ListGroupItem>
                                            <ListGroupItem tag="button" className="list-group-item-action"><i className="ri-equalizer-line align-middle me-2"></i>Filter Message</ListGroupItem>
                                            <ListGroupItem tag="button" className="list-group-item-action"><i className="ri-delete-bin-5-line align-middle me-2"></i>Delete Message</ListGroupItem>
                                            <ListGroupItem tag="button" className="list-group-item-action" disabled><i className="ri-forbid-line align-middle me-2"></i>Block "Mark Spencer</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ListButtonExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="Flush List" />

                                <CardBody>
                                    <p className="text-muted">Use <code>list-group-flush</code> class to remove some borders and rounded corners to render list group items.</p>
                                    <div className="live-preview">
                                        <ListGroup flush>
                                            <ListGroupItem>Send the billing agreement</ListGroupItem>
                                            <ListGroupItem>Send over all the documentation.</ListGroupItem>
                                            <ListGroupItem>Meeting with daron to review the intake form</ListGroupItem>
                                            <ListGroupItem>Check uikings theme and give customer support</ListGroupItem>
                                            <ListGroupItem>Start making a presentation</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <FlushListExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col xl={12}>
                            <Card>
                                <PreviewCardHeader title="Horizontal List" />

                                <CardBody>
                                    <p className="text-muted">Use <code>list-group-horizontal</code> class to change the layout of list group items from vertical to horizontal across all breakpoints.</p>
                                    <div className="live-preview">
                                        <ListGroup className="mb-3" horizontal>
                                            <ListGroupItem>Inbox</ListGroupItem>
                                            <ListGroupItem>Work</ListGroupItem>
                                            <ListGroupItem>Shopping</ListGroupItem>
                                        </ListGroup>
                                        <ListGroup className="justify-content-center mb-3" horizontal>
                                            <ListGroupItem>Inbox</ListGroupItem>
                                            <ListGroupItem>Work</ListGroupItem>
                                            <ListGroupItem>Shopping</ListGroupItem>
                                        </ListGroup>
                                        <ListGroup className="justify-content-end" horizontal>
                                            <ListGroupItem>Inbox</ListGroupItem>
                                            <ListGroupItem>Work</ListGroupItem>
                                            <ListGroupItem>Shopping</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <HorizontalListExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="Contextual Classes" />

                                <CardBody>
                                      <p className="text-muted">Use contextual classes to style list items with a stateful background and color.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem >Dapibus ac facilisis in</ListGroupItem>
                                            <ListGroupItem color="primary">A simple primary list group item</ListGroupItem>
                                            <ListGroupItem color="secondary">A simple secondary list group item</ListGroupItem>
                                            <ListGroupItem color="success">A simple success list group item</ListGroupItem>
                                            <ListGroupItem color="danger">A simple danger list group item</ListGroupItem>
                                            <ListGroupItem color="warning">A simple warning list group item</ListGroupItem>
                                            <ListGroupItem color="info">A simple info list group item</ListGroupItem>
                                            <ListGroupItem color="light">A simple light list group item</ListGroupItem>
                                            <ListGroupItem color="dark">A simple dark list group item</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ContextualClassExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="Contextual Classes with Link" />

                                <CardBody>
                                    <p className="text-muted">Contextual classes also work with  <code>.list-group-item-action</code>. Note the addition of the hover styles here not present in the previous example.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action">A simple default list group item</ListGroupItem>
                                            <ListGroupItem color="primary" tag="a" to="#" className="list-group-item-action">A simple primary list group item</ListGroupItem>
                                            <ListGroupItem color="secondary" tag="a" to="#" className="list-group-item-action">A simple secondary list group item</ListGroupItem>
                                            <ListGroupItem color="success" tag="a" to="#" className="list-group-item-action">A simple success list group item</ListGroupItem>
                                            <ListGroupItem color="danger" tag="a" to="#" className="list-group-item-action">A simple danger list group item</ListGroupItem>
                                            <ListGroupItem color="warning" tag="a" to="#" className="list-group-item-action">A simple warning list group item</ListGroupItem>
                                            <ListGroupItem color="info" tag="a" to="#" className="list-group-item-action">A simple info list group item</ListGroupItem>
                                            <ListGroupItem color="light" tag="a" to="#" className="list-group-item-action">A simple light list group item</ListGroupItem>
                                            <ListGroupItem color="dark" tag="a" to="#" className="list-group-item-action">A simple dark list group item</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ContextualLinkExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>


                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="Colored Lists" />

                                <CardBody>
                                    <p className="text-muted">Use <code>list-group-fill-</code> with modifier class to style list items with a stateful background and color.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem >Dapibus ac facilisis in</ListGroupItem>
                                            <ListGroupItem className="list-group-fill-primary">A simple primary list group item</ListGroupItem>
                                            <ListGroupItem className="list-group-fill-secondary">A simple secondary list group item</ListGroupItem>
                                            <ListGroupItem className="list-group-fill-success">A simple success list group item</ListGroupItem>
                                            <ListGroupItem className="list-group-fill-danger">A simple danger list group item</ListGroupItem>
                                            <ListGroupItem className="list-group-fill-warning">A simple warning list group item</ListGroupItem>
                                            <ListGroupItem className="list-group-fill-info">A simple info list group item</ListGroupItem>
                                            <ListGroupItem className="list-group-fill-light">A simple light list group item</ListGroupItem>
                                            <ListGroupItem className="list-group-fill-dark">A simple dark list group item</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ColoredListExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                    </Row>


                    <Row>
                        <Col xl={12}>
                            <Card>
                                <PreviewCardHeader title="Custom Content" />

                                <CardBody>
                                    <p className="text-muted">Add nearly any HTML within, even for linked list groups like the one below, with the help of <Link to="https://getbootstrap.com/docs/5.1/utilities/flex/">flexbox utilities</Link>.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action active">
                                                <div className="float-end">
                                                    Pending
                                                </div>
                                                <div className="d-flex mb-2 align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar1} alt="" className="avatar-sm rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-3">
                                                        <h5 className="list-title fs-15 mb-1">Charlie Pritchard</h5>
                                                        <p className="list-text mb-0 fs-12">12 min Ago</p>
                                                    </div>
                                                </div>
                                                <p className="list-text mb-0">They all have something to say beyond the words on the page. They can come across as casual or neutral, exotic or graphic. That's why it's important to think about your message, then choose a font that fits. Cosby sweater eu banh mi, qui irure terry richardson ex squid.</p>
                                            </ListGroupItem>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action">
                                                <div className="float-end">
                                                    Pending
                                                </div>
                                                <div className="d-flex mb-2 align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar2} alt="" className="avatar-sm rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-3">
                                                        <h5 className="list-title fs-15 mb-1">Dominic Charlton</h5>
                                                        <p className="list-text mb-0 fs-12">12 min Ago</p>
                                                    </div>
                                                </div>
                                                <p className="list-text mb-0">Trust fund seitan letterpress, keytar raw denim keffiyeh etsy art party before they sold out master cleanse gluten-free squid scenester freegan cosby sweater. Fanny pack portland seitan DIY, art party locavore wolf cliche high life echo park Austin.</p>
                                            </ListGroupItem>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action">
                                                <div className="float-end">
                                                    Rejected
                                                </div>
                                                <div className="d-flex mb-2 align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar3} alt="" className="avatar-sm rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-3">
                                                        <h5 className="list-title fs-15 mb-1">Declan Long</h5>
                                                        <p className="list-text mb-0 fs-12">12 min Ago</p>
                                                    </div>
                                                </div>
                                                <p className="list-text mb-0">Big July earthquakes confound zany experimental vow. My girl wove six dozen plaid jackets before she quit. Six big devils from Japan quickly forgot how to waltz. try again until it looks right, and each assumenda labore aes Homo nostrud organic, assumenda labore aesthetic magna elements, buttons, everything.</p>
                                            </ListGroupItem>
                                            <ListGroupItem tag="a" to="#" className="list-group-item-action">
                                                <div className="float-end">
                                                    Successful
                                                </div>
                                                <div className="d-flex mb-2 align-items-center">
                                                    <div className="flex-shrink-0">
                                                        <img src={avatar4} alt="" className="avatar-sm rounded-circle" />
                                                    </div>
                                                    <div className="flex-grow-1 ms-3">
                                                        <h5 className="list-title fs-15 mb-1">Angela Bernier</h5>
                                                        <p className="list-text mb-0 fs-12">5 days Ago</p>
                                                    </div>
                                                </div>
                                                <p className="list-text mb-0">Just like in the image where we talked about using multiple fonts, you can see that the background in this graphic design is blurred. Whenever you put text on top of an image, it’s important that your viewers can understand the text, and sometimes that means applying a gaussian readable.</p>
                                            </ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "300px" }}>
                                            <code>
                                                <CustomContentExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="List with Badges" />
                                <CardBody>
                                    <p className="text-muted">Use badges to any list group item to show unread counts, activity, and more with the help of some <Link to="https://getbootstrap.com/docs/5.1/utilities/">utilities</Link>.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem className="d-flex justify-content-between align-items-center">
                                                Send the billing agreement <span className="badge bg-success">High</span>
                                            </ListGroupItem>
                                            <ListGroupItem className="d-flex justify-content-between align-items-center">
                                                Send over all the documentation
                                            </ListGroupItem>
                                            <ListGroupItem className="d-flex justify-content-between align-items-center">
                                                Meeting with daron to review the intake form <span className="badge bg-danger">Low</span>
                                            </ListGroupItem>
                                            <ListGroupItem className="d-flex justify-content-between align-items-center">
                                                Check uikings theme and give customer support <span className="badge bg-secondary">Medium</span>
                                            </ListGroupItem>
                                            <ListGroupItem className="d-flex justify-content-between align-items-center">
                                                Start making a presentation <span className="badge bg-success">High</span>
                                            </ListGroupItem>
                                        </ListGroup>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <ListBadgeExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="List with Checkboxs" />

                                <CardBody>
                                    <p className="text-muted">Use Bootstrap’s checkboxes within list group items and customize as needed. </p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input me-1" type="checkbox" value="" />
                                                Declined Payment
                                            </ListGroupItem>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input me-1" type="checkbox" value="" defaultChecked />
                                                Delivery Error
                                            </ListGroupItem>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input me-1" type="checkbox" value="" defaultChecked />
                                                Wrong Amount
                                            </ListGroupItem>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input me-1" type="checkbox" value="" />
                                                Wrong Address
                                            </ListGroupItem>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input me-1" type="checkbox" value="" />
                                                Wrong UX/UI Solution
                                            </ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ListCheckboxExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="List with Radios" />
                                <CardBody>
                                    <p className="text-muted">Use Bootstrap’s radios within list group items and customize as needed. </p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" />
                                                Declined Payment
                                            </ListGroupItem>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" defaultChecked />
                                                Delivery Error
                                            </ListGroupItem>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" />
                                                Wrong Amount
                                            </ListGroupItem>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" />
                                                Wrong Address
                                            </ListGroupItem>
                                            <ListGroupItem tag="label">
                                                <Input className="form-check-input fs-14 mt-0 align-middle me-1" name="exampleRadios" type="radio" value="" />
                                                Wrong UX/UI Solution
                                            </ListGroupItem>
                                        </ListGroup>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ListRadioExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="List with Icon" />
                                <CardBody>
                                    <p className="text-muted">Use icons to any list group item to show icons to list group items.</p>
                                    <div className="live-preview">
                                        <ListGroup>
                                            <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Send the billing agreement</ListGroupItem>
                                            <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Send over all the documentation.</ListGroupItem>
                                            <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Meeting with daron to review the intake form</ListGroupItem>
                                            <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Check uikings theme and give customer support</ListGroupItem>
                                            <ListGroupItem><i className="mdi mdi-check-bold align-middle lh-1 me-2"></i> Start making a presentation</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ListIconExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="List with Numbered" />

                                <CardBody>
                                    <p className="text-muted">Use <code>list-group-numbered</code> class (optionally use an <code>&lt;ol&gt;</code> element) to show numbered list group items. </p>
                                    <div className="live-preview">
                                        <ListGroup className="list-group-numbered">
                                            <ListGroupItem>Send the billing agreement</ListGroupItem>
                                            <ListGroupItem>Send over all the documentation.</ListGroupItem>
                                            <ListGroupItem>Meeting with daron to review the intake form</ListGroupItem>
                                            <ListGroupItem>Check uikings theme and give customer support</ListGroupItem>
                                            <ListGroupItem>Start making a presentation</ListGroupItem>
                                        </ListGroup>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ListNumberedExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={4} xl={6}>
                            <Card>
                                <PreviewCardHeader title="Custom Content Lists" />
                                <CardBody>
                                    <p>Add nearly any HTML within, even for linked list groups like the one below, with the help of <Link to="https://getbootstrap.com/docs/5.1/utilities/flex/">flexbox utilities</Link>.</p>                           <div className="live-preview">
                                        <SimpleBar style={{ maxHeight: "215px" }}>
                                            <ListGroup className="mb-1">
                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center">
                                                        <div className="flex-grow-1">
                                                            <div className="d-flex">
                                                                <div className="flex-shrink-0 avatar-xs">
                                                                    <div className="avatar-title bg-soft-danger text-danger rounded">
                                                                        <i className="ri-netflix-fill"></i>
                                                                    </div>
                                                                </div>
                                                                <div className="flex-shrink-0 ms-2">
                                                                    <h6 className="fs-14 mb-0">Netfilx</h6>
                                                                    <small className="text-muted">2 min Ago</small>
                                                                </div>
                                                            </div>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <span className="text-danger">-$25.50</span>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center">
                                                        <div className="flex-grow-1">
                                                            <div className="d-flex">
                                                                <div className="flex-shrink-0 avatar-xs">
                                                                    <div className="avatar-title bg-soft-success text-success rounded">
                                                                        <i className="ri-spotify-fill"></i>
                                                                    </div>
                                                                </div>
                                                                <div className="flex-shrink-0 ms-2">
                                                                    <h6 className="fs-14 mb-0">Spotify</h6>
                                                                    <small className="text-muted">5 days Ago</small>
                                                                </div>
                                                            </div>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <span className="text-success">$48.25</span>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center">
                                                        <div className="flex-grow-1">
                                                            <div className="d-flex">
                                                                <div className="flex-shrink-0">
                                                                    <img src={avatar2} alt="" className="avatar-xs rounded" />
                                                                </div>
                                                                <div className="flex-shrink-0 ms-2">
                                                                    <h6 className="fs-14 mb-0">Emily Slater</h6>
                                                                    <small className="text-muted">8 days Ago</small>
                                                                </div>
                                                            </div>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <span className="text-success">$354.90</span>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center">
                                                        <div className="flex-grow-1">
                                                            <div className="d-flex">
                                                                <div className="flex-shrink-0 avatar-xs">
                                                                    <div className="avatar-title bg-soft-secondary text-secondary rounded">
                                                                        <i className="ri-paypal-fill"></i>
                                                                    </div>
                                                                </div>
                                                                <div className="flex-shrink-0 ms-2">
                                                                    <h6 className="fs-14 mb-0">Paypal</h6>
                                                                    <small className="text-muted">1 month Ago</small>
                                                                </div>
                                                            </div>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <span className="text-danger">-$12.22</span>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                                <ListGroupItem>
                                                    <div className="d-flex align-items-center">
                                                        <div className="flex-grow-1">
                                                            <div className="d-flex">
                                                                <div className="flex-shrink-0">
                                                                    <img src={avatar3} alt="" className="avatar-xs rounded" />
                                                                </div>
                                                                <div className="flex-shrink-0 ms-2">
                                                                    <h6 className="fs-14 mb-0">Harvey Wells</h6>
                                                                    <small className="text-muted">4 month Ago</small>
                                                                </div>
                                                            </div>
                                                        </div>
                                                        <div className="flex-shrink-0">
                                                            <span className="text-success">-$459.78</span>
                                                        </div>
                                                    </div>
                                                </ListGroupItem>
                                            </ListGroup>
                                        </SimpleBar>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "215px" }}>
                                            <code>
                                                <CustomListExample />
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

export default UiList;
