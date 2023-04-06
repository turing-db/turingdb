import React from 'react';
import { Button, Card, CardBody, Col, Container, DropdownItem, DropdownMenu, DropdownToggle, Row, UncontrolledButtonDropdown, ButtonGroup, ButtonToolbar, Input, Spinner } from 'reactstrap';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import { Link } from 'react-router-dom';

import { DefaultButtonsExample, OutlineButtonsExample, RoundedButtonsExample, SoftButtonsExample, GradientButtonsExample, AnimationButtonsExample, LabelButtonsExample, SizeButtonsExample, WidthButtonsExample, TagButtonsExample, GridButtonsExample, CheckButtonsExample, GroupButtonsExample, IconButtonsExample, ToolbarButtonsExample, GroupsizingButtonsExample, VerticalButtonsExample, GhostButtonsExample, LoadingbuttonsExample, BorderButtonsExample, CustomToggleButtonsExample } from './UiButtonCode';

const UiButtons = () => {
    document.title="Buttons | Velzon - React Admin & Dashboard Template";
    const favouriteBtn = (ele) => {
        if (ele.closest("button").classList.contains("active")) {
            ele.closest("button").classList.remove("active");
        } else {
            ele.closest("button").classList.add("active");
        }
    };
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Buttons" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Default Buttons" />

                                <CardBody>
                                    <p className="text-muted">Use the <code>btn</code> class to show the default button style.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary"> Primary </Button>
                                            <Button color="secondary"> Secondary </Button>
                                            <Button color="success"> Success </Button>
                                            <Button color="info"> Info </Button>
                                            <Button color="warning"> Warning </Button>
                                            <Button color="danger"> Danger </Button>
                                            <Button color="dark"> Dark </Button>
                                            <Button color="link"> Link </Button>
                                            <Button color="light"> Light </Button>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <DefaultButtonsExample />
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
                                <PreviewCardHeader title="Outline Buttons" />
                                <CardBody>
                                    <p className="text-muted">Use <code>btn-outline-</code>  class with the below-mentioned variation to create a button with the outline.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary" outline > Primary </Button>
                                            <Button color="secondary" outline > Secondary </Button>
                                            <Button color="success" outline > Success </Button>
                                            <Button color="info" outline > Info </Button>
                                            <Button color="warning" outline > Warning </Button>
                                            <Button color="danger" outline > Danger </Button>
                                            <Button color="dark" outline > Dark </Button>
                                            <Button color="light" outline > Light </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <OutlineButtonsExample />
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
                                <PreviewCardHeader title="Rounded Buttons" />
                                <CardBody>
                                    <p className="text-muted">Use the <code >rounded-pill </code>class to make a rounded button.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary" className="rounded-pill"> Primary </Button>
                                            <Button color="secondary" className="rounded-pill"> Secondary </Button>
                                            <Button color="success" className="rounded-pill"> Success </Button>
                                            <Button color="info" className="rounded-pill"> Info </Button>
                                            <Button color="warning" className="rounded-pill"> Warning </Button>
                                            <Button color="danger" className="rounded-pill"> Danger </Button>
                                            <Button color="dark" className="rounded-pill"> Dark </Button>
                                            <Button color="light" className="rounded-pill"> Light </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <RoundedButtonsExample />
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
                                <PreviewCardHeader title="Soft Buttons" />
                                <CardBody>
                                    <p className="text-muted">Use <code>btn-soft-</code>  class with the below-mentioned variation to create a button with the soft background.</p>                                        <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button className="btn-soft-primary"> Primary </Button>
                                            <Button className="btn-soft-secondary"> Secondary </Button>
                                            <Button className="btn-soft-success"> Success </Button>
                                            <Button className="btn-soft-info"> Info </Button>
                                            <Button className="btn-soft-warning"> Warning </Button>
                                            <Button className="btn-soft-danger"> Danger </Button>
                                            <Button className="btn-soft-dark"> Dark </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <SoftButtonsExample />
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
                                <PreviewCardHeader title="Ghost Buttons" />
                                <CardBody>
                                    <p className="text-muted">Use <code>btn-ghost-</code>  class with the below-mentioned variation to create a button with the transparent background.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary" outline className="btn btn-ghost-primary">Primary</Button>
                                            <Button color="secondary" outline className="btn btn-ghost-secondary">Secondary</Button>
                                            <Button color="success" outline className="btn btn-ghost-success">Success</Button>
                                            <Button color="info" outline className="btn btn-ghost-info">Info</Button>
                                            <Button color="warning" outline className="btn btn-ghost-warning">Warning</Button>
                                            <Button color="danger" outline className="btn btn-ghost-danger">Danger</Button>
                                            <Button color="dark" outline className="btn btn-ghost-dark">Dark</Button>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <GhostButtonsExample />
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
                                <PreviewCardHeader title="Gradient Buttons" />

                                <CardBody>
                                    <p className="text-muted">Use <code >bg-gradient </code>class to create a gradient button.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary" className="bg-gradient"> Primary </Button>
                                            <Button color="secondary" className="bg-gradient"> Secondary </Button>
                                            <Button color="success" className="bg-gradient"> Success </Button>
                                            <Button color="info" className="bg-gradient"> Info </Button>
                                            <Button color="warning" className="bg-gradient"> Warning </Button>
                                            <Button color="danger" className="bg-gradient"> Danger </Button>
                                            <Button color="dark" className="bg-gradient"> Dark </Button>
                                            <Button color="light" className="bg-gradient"> Light </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <GradientButtonsExample />
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
                                <PreviewCardHeader title="Animation Buttons" />
                                <CardBody>
                                    <p className="text-muted">Use <code >bg-animation </code>class to create an animated button.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary" className="btn-animation" data-text="Primary"> <span>Primary</span> </Button>
                                            <Button color="secondary" className="btn-animation" data-text="Secondary"> <span>Secondary</span> </Button>
                                            <Button color="success" className="btn-animation" data-text="Success"> <span>Success</span> </Button>
                                            <Button color="info" className="btn-animation" data-text="Info"> <span>Info</span> </Button>
                                            <Button color="warning" className="btn-animation" data-text="Warning"> <span>Warning</span> </Button>
                                            <Button color="danger" className="btn-animation" data-text="Danger"> <span>Danger</span> </Button>
                                            <Button color="dark" className="btn-animation" data-text="Dark"> <span>Dark</span> </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <AnimationButtonsExample />
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
                                <PreviewCardHeader title="Buttons with Label" />

                                <CardBody>
                                    <p className="text-muted">Use <code >btn-label </code>class to create a button with the label.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={4}>
                                                <div className="d-flex flex-wrap gap-2 mb-3 mb-lg-0">
                                                    <Button color="primary" className="btn-label"> <i className="ri-user-smile-line label-icon align-middle fs-16 me-2"></i> Primary </Button>
                                                    <Button color="success" className="btn-label"> <i className="ri-check-double-line label-icon align-middle fs-16 me-2"></i> Success </Button>
                                                    <Button color="warning" className="btn-label"> <i className="ri-error-warning-line label-icon align-middle fs-16 me-2"></i> Warning </Button>
                                                </div>
                                            </Col>
                                            <Col lg={4}>
                                                <div className="d-flex flex-wrap gap-2 mb-3 mb-lg-0">
                                                    <Button color="primary" className="btn-label rounded-pill"> <i className="ri-user-smile-line label-icon align-middle fs-16 me-2"></i> Primary </Button>
                                                    <Button color="success" className="btn-label rounded-pill"> <i className="ri-check-double-line label-icon align-middle fs-16 me-2"></i> Success </Button>
                                                    <Button color="warning" className="btn-label rounded-pill"> <i className="ri-error-warning-line label-icon align-middle fs-16 me-2"></i> Warning </Button>
                                                </div>
                                            </Col>
                                            <Col lg={4}>
                                                <div className="d-flex flex-wrap gap-2">
                                                    <Button color="primary" className="btn-label right"> <i className="ri-user-smile-line label-icon align-middle fs-16 ms-2"></i> Primary </Button>
                                                    <Button color="success" className="btn-label right rounded-pill"> <i className="ri-check-double-line label-icon align-middle rounded-pill fs-16 ms-2"></i> Success </Button>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <LabelButtonsExample />
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
                                <PreviewCardHeader title="Load More Buttons" />
                                <CardBody>
                                    <p className="text-muted">Example of loading buttons.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={6}>
                                                <div className="hstack flex-wrap gap-2 mb-3 mb-lg-0">
                                                    <Button color="primary" className="btn-load" outline>
                                                        <span className="d-flex align-items-center">
                                                            <Spinner size="sm" className="flex-shrink-0"> Loading... </Spinner>
                                                            <span className="flex-grow-1 ms-2">
                                                                Loading...
                                                            </span>
                                                        </span>
                                                    </Button>
                                                    <Button color="success" className="btn-load">
                                                        <span className="d-flex align-items-center">
                                                            <Spinner size="sm" className="flex-shrink-0"> Loading... </Spinner>
                                                            <span className="flex-grow-1 ms-2">
                                                                Loading...
                                                            </span>
                                                        </span>
                                                    </Button>
                                                    <Button color="secondary" className="btn-load" outline>
                                                        <span className="d-flex align-items-center">
                                                            <Spinner size="sm" type="grow" className="flex-shrink-0"> Loading... </Spinner>
                                                            <span className="flex-grow-1 ms-2">
                                                                Loading...
                                                            </span>
                                                        </span>
                                                    </Button>
                                                    <Button color="danger" className="btn-load">
                                                        <span className="d-flex align-items-center">
                                                            <Spinner size="sm" type="grow" className="flex-shrink-0"> Loading... </Spinner>
                                                            <span className="flex-grow-1 ms-2">
                                                                Loading...
                                                            </span>
                                                        </span>
                                                    </Button>
                                                </div>
                                            </Col>

                                            <Col lg={6}>
                                                <div className="d-flex flex-wrap gap-2 mb-3 mb-lg-0">
                                                    <Button color="primary" className="btn-load" outline>
                                                        <span className="d-flex align-items-center">
                                                            <span className="flex-grow-1 me-2">
                                                                Loading...
                                                            </span>
                                                            <Spinner size="sm" className="flex-shrink-0" role="status"> Loading... </Spinner>
                                                        </span>
                                                    </Button>
                                                    <Button color="success" className="btn-load">
                                                        <span className="d-flex align-items-center">
                                                            <span className="flex-grow-1 me-2">
                                                                Loading...
                                                            </span>
                                                            <Spinner size="sm" className="flex-shrink-0" role="status"> Loading... </Spinner>
                                                        </span>
                                                    </Button>
                                                    <Button color="warning" className="btn-load" outline>
                                                        <span className="d-flex align-items-center">
                                                            <span className="flex-grow-1 me-2">
                                                                Loading...
                                                            </span>
                                                            <Spinner size="sm" type="grow" className="flex-shrink-0" role="status"> Loading... </Spinner>
                                                        </span>
                                                    </Button>
                                                    <Button color="info" className="btn-load">
                                                        <span className="d-flex align-items-center">
                                                            <span className="flex-grow-1 me-2">
                                                                Loading...
                                                            </span>
                                                            <Spinner size="sm" type="grow" className="flex-shrink-0" role="status"> Loading... </Spinner>
                                                        </span>
                                                    </Button>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <LoadingbuttonsExample />
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
                                <PreviewCardHeader title="Border Buttons" />
                                <CardBody>
                                    <p className="text-muted">Example of simple bottom borderd buttons.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={6}>
                                                <div className="hstack flex-wrap gap-2 mb-3 mb-lg-0">
                                                    <Button color="primary" className="btn-border">Primary</Button>
                                                    <Button color="secondary" className="btn-border">Secondary</Button>
                                                    <Button color="success" className="btn-border">Success</Button>
                                                    <Button color="warning" className="btn-border">Warning</Button>
                                                    <Button color="danger" className="btn-border">Danger</Button>
                                                </div>
                                            </Col>
                                            <Col lg={6}>
                                                <div className="hstack flex-wrap gap-2 mb-3 mb-lg-0">
                                                    <Button color="primary" outline className="btn-border">Primary</Button>
                                                    <Button color="secondary" outline className="btn-border">Secondary</Button>
                                                    <Button color="success" outline className="btn-border">Success</Button>
                                                    <Button className="btn-soft-warning btn-border">Warning</Button>
                                                    <Button className="btn-soft-danger btn-border">Danger</Button>
                                                    <Button className="btn-soft-dark btn-border">Dark</Button>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <BorderButtonsExample />
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
                                <PreviewCardHeader title="Custom Toggle Buttons" />
                                <CardBody>
                                    <p className="text-muted">Example of toggle buttons.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={6}>
                                                <div className="d-flex flex-wrap align-items-center gap-2">
                                                    <Button color='primary' className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on"><i className="ri-alarm-line align-bottom"></i> Active</span>
                                                        <span className="icon-off">Unactive</span>
                                                    </Button>
                                                    <Button color='secondary' className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on"><i className="ri-user-add-line align-bottom me-1"></i> Connect</span>
                                                        <span className="icon-off"><i className="ri-check-fill align-bottom me-1"></i> Unconnect</span>
                                                    </Button>
                                                    <Button color='success' className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on"><i className="ri-thumb-up-line align-bottom me-1"></i> Yes</span>
                                                        <span className="icon-off"><i className="ri-thumb-down-line align-bottom me-1"></i> No</span>
                                                    </Button>
                                                    <Button color='warning' className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on"><i className="ri-add-line align-bottom me-1"></i> Follow</span>
                                                        <span className="icon-off"><i className="ri-user-unfollow-line align-bottom me-1"></i> Unfollow</span>
                                                    </Button>
                                                    <Button color='danger' className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on">On</span>
                                                        <span className="icon-off">Off</span>
                                                    </Button>
                                                    <Button color='dark' className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on"><i className="ri-bookmark-line align-bottom me-1"></i> Bookmark</span>
                                                        <span className="icon-off"><i className="ri-bookmark-3-fill align-bottom me-1"></i> Unbookmark</span>
                                                    </Button>
                                                </div>
                                            </Col>
                                            <Col lg={6}>
                                                <div className="d-flex flex-wrap align-items-center gap-2">
                                                    <Button color="primary" outline className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on">Active</span>
                                                        <span className="icon-off">Unactive</span>
                                                    </Button>
                                                    <Button color="secondary" outline className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on"><i className="ri-add-line align-bottom me-1"></i> Follow</span>
                                                        <span className="icon-off"><i className="ri-user-unfollow-line align-bottom me-1"></i> Unfollow</span>
                                                    </Button>
                                                    <Button color="success" outline className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on">On</span>
                                                        <span className="icon-off">Off</span>
                                                    </Button>
                                                    <Button color="warning" outline className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on">Follow</span>
                                                        <span className="icon-off">Unfollow</span>
                                                    </Button>
                                                    <Button color="danger" outline className="custom-toggle" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on">On</span>
                                                        <span className="icon-off">Off</span>
                                                    </Button>
                                                    <Button color="dark" outline className="custom-toggle active" onClick={(e) => favouriteBtn(e.target)}>
                                                        <span className="icon-on"><i className="ri-bookmark-line align-bottom"></i></span>
                                                        <span className="icon-off"><i className="ri-bookmark-3-fill align-bottom"></i></span>
                                                    </Button>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <CustomToggleButtonsExample />
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
                                <PreviewCardHeader title="Buttons Sizes" />

                                <CardBody>
                                    <p className="text-muted">Use <code >btn-lg</code> class to create a large size button and <code >btn-sm</code> class to create a small size button.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap align-items-center gap-2">
                                            <Button size="lg" color="primary"> Large button </Button>
                                            <Button size="lg" color="light"> Large button </Button>

                                            <Button size="sm" color="primary"> Small button </Button>
                                            <Button size="sm" color="light"> Small button </Button>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <SizeButtonsExample />
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
                                <PreviewCardHeader title="Buttons Width" />

                                <CardBody>
                                    <p className="text-muted">Use <code >w-xs,w-sm,w-md,w-lg</code> class to make different sized buttons respectively.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary" className="w-xs"> Xs </Button>
                                            <Button color="danger" className="w-sm"> Small </Button>
                                            <Button color="warning" className="w-md"> Medium </Button>
                                            <Button color="success" className="w-lg"> Large </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <WidthButtonsExample />
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
                                <PreviewCardHeader title="Buttons Tag" />

                                <CardBody>
                                    <p className="text-muted">Use <code >btn</code> class with different HTML elements. (though some browsers may apply a slightly different rendering)</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Link to="#" className="btn btn-primary"> Link </Link>
                                            <Button color="success" type="submit"> Button </Button>
                                            <Button color="info" type="button"> Input </Button>
                                            <Button color="success" type="submit"> Submit </Button>
                                            <Button color="warning" type="reset"> Reset </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" >
                                            <code>
                                                <TagButtonsExample />
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
                                <PreviewCardHeader title="Buttons Toggle Status" />

                                <CardBody>
                                    <p className="text-muted">
                                        Use <code>data-bs-toggle="button"</code> to toggle a button’s active state.
                                    </p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Button color="primary" data-bs-toggle="button" aria-pressed="false">
                                                Single toggle
                                            </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>&lt;!-- Toggle Button Status --&gt;
                                                &lt;button type=&quot;button&quot; className=&quot;btn btn-primary waves-effect waves-light&quot; data-bs-toggle=&quot;button&quot; aria-pressed=&quot;false&quot;&gt;
                                                Single toggle
                                                &lt;/button&gt;</code></pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Buttons Grid" />

                                <CardBody>

                                    <p className="text-muted">
                                        Use <code>d-grid</code> class to create a full-width block button.
                                    </p>
                                    <div className="live-preview">
                                        <div className="d-grid gap-2">
                                            <Button color="primary"> Button </Button>
                                            <Button color="primary"> Button </Button>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <GridButtonsExample />
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
                                <PreviewCardHeader title="Checkbox & Radio Buttons" />

                                <CardBody>
                                    <p className="text-muted">
                                        Combine button-like <code>checkbox and radio</code> <a href="https://getbootstrap.com/docs/5.1/forms/checks-radios/">toggle buttons</a> into a seamless looking button group.
                                    </p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap align-items-center gap-2">
                                            <ButtonGroup>
                                                <Input type="checkbox" className="btn-check" id="btncheck1" defaultChecked="" />
                                                <Button color="primary" className="mb-0" id="btncheck1"> Checkbox 1 </Button>

                                                <Input type="checkbox" className="btn-check" id="btncheck2" />
                                                <Button color="primary" className="mb-0" id="btncheck2"> Checkbox 2 </Button>

                                                <Input type="checkbox" className="btn-check" id="btncheck3" />
                                                <Button color="primary" className="mb-0" id="btncheck3"> Checkbox 3 </Button>
                                            </ButtonGroup>

                                            <ButtonGroup>
                                                <Input type="radio" className="btn-check" name="btnradio" id="btnradio1" defaultChecked="" />
                                                <Button color="secondary" className="mb-0" id="btnradio1" outline> Radio 1 </Button>

                                                <Input type="radio" className="btn-check" name="btnradio" id="btnradio2" />
                                                <Button color="secondary" className="mb-0" id="btnradio2" outline> Radio 2 </Button>

                                                <Input type="radio" className="btn-check" name="btnradio" id="btnradio3" />
                                                <Button color="secondary" className="mb-0" id="btnradio3" outline> Radio 3 </Button>

                                            </ButtonGroup>
                                        </div>
                                    </div>
                                    <div className="d-none code-view" >
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <CheckButtonsExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col lg={6}>
                            <Card>
                                <PreviewCardHeader title="Buttons Group" />

                                <CardBody>

                                    <p className="text-muted">Use the <code>btn-group </code> class in the parent class to wrap a series of buttons.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col sm={6}>
                                                <ButtonGroup>
                                                    <Button color="primary"> Left </Button>
                                                    <Button color="primary"> Middle </Button>
                                                    <Button color="primary"> Right </Button>
                                                </ButtonGroup>
                                            </Col>
                                            <Col sm={6}>
                                                <ButtonGroup className="mt-4 mt-sm-0">
                                                    <Button color="light" className="btn-icon"> <i className="ri-align-right" /> </Button>
                                                    <Button color="light" className="btn-icon"> <i className="ri-align-center" /> </Button>
                                                    <Button color="light" className="btn-icon"> <i className="ri-align-left" /> </Button>
                                                </ButtonGroup>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <GroupButtonsExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col lg={6}>
                            <Card>
                                <PreviewCardHeader title="Icon Buttons" />

                                <CardBody>
                                    <p className="text-muted">Use <code>btn-icon</code> class to wrap icon in button</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col sm={6}>
                                                <div className="hstack gap-2 ">
                                                    <Button color="primary" className="btn-icon"> <i className="ri-map-pin-line" /> </Button>
                                                    <Button color="danger" className="btn-icon"> <i className="ri-delete-bin-5-line" /> </Button>
                                                    <Button color="success" className="btn-icon"> <i className="ri-check-double-line" /> </Button>
                                                    <Button color="light" className="btn-icon"> <i className="ri-brush-2-fill" /> </Button>
                                                </div>
                                            </Col>
                                            <Col sm={6}>
                                                <div className="hstack gap-2 mt-4 mt-sm-0">
                                                    <Button color="primary" className="btn-icon" outline> <i className="ri-24-hours-fill" /> </Button>
                                                    <Button color="danger" className="btn-icon" outline> <i className="ri-customer-service-2-line" /> </Button>
                                                    <Button color="success" className="btn-icon" outline> <i className="ri-mail-send-line" /> </Button>
                                                    <Button color="warning" className="btn-icon" outline> <i className="ri-menu-2-line" /> </Button>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" >
                                            <code>
                                                <IconButtonsExample />
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
                                <PreviewCardHeader title="Buttons Toolbar" />
                                <CardBody>
                                    <p className="text-muted">Use <code>btn-toolbar</code> class to combine sets of button groups into more complex components.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3">
                                            <ButtonToolbar>
                                                <ButtonGroup className="me-2">
                                                    <Button color="light">1</Button>
                                                    <Button color="light">2</Button>
                                                    <Button color="light">3</Button>
                                                    <Button color="light">4</Button>
                                                </ButtonGroup>
                                                <ButtonGroup className="me-2">
                                                    <Button color="light">5</Button>
                                                    <Button color="light">6</Button>
                                                    <Button color="light">7</Button>

                                                </ButtonGroup>
                                                <ButtonGroup>
                                                    <Button color="light">8</Button>
                                                </ButtonGroup>
                                            </ButtonToolbar>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ToolbarButtonsExample />
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
                                <PreviewCardHeader title="Button Group Sizing" />

                                <CardBody>
                                    <p className="text-muted">Use <code>btn-group-</code> class with the below-mentioned variation to set the different sizes of button groups.</p>

                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap align-items-center gap-2">
                                            <ButtonGroup size="lg">
                                                <Button color="primary">Left</Button>
                                                <Button color="primary">Middle</Button>
                                                <Button color="primary">Right</Button>
                                            </ButtonGroup>
                                            <ButtonGroup className="mt-2">
                                                <Button color="light">Left</Button>
                                                <Button color="light">Middle</Button>
                                                <Button color="light">Right</Button>
                                            </ButtonGroup>
                                            <ButtonGroup size="sm" className="mt-2">
                                                <Button color="secondary">Left</Button>
                                                <Button color="secondary">Middle</Button>
                                                <Button color="secondary">Right</Button>
                                            </ButtonGroup>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <GroupsizingButtonsExample />
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
                                <PreviewCardHeader title="Vertical Variation" />

                                <CardBody>
                                    <p className="text-muted">Make a set of buttons appear <code>vertically</code> stacked .Split button dropdowns are not supported here.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <div className="col-auto">
                                                <ButtonGroup>
                                                    <Button color="primary">1</Button>
                                                    <Button color="primary">2</Button>
                                                    <UncontrolledButtonDropdown id="btnGroupDrop1">
                                                        <DropdownToggle color="primary" caret>
                                                            Dropdown
                                                        </DropdownToggle>
                                                        <DropdownMenu>
                                                            <DropdownItem> Dropdown link </DropdownItem>
                                                            <DropdownItem> Dropdown link </DropdownItem>
                                                        </DropdownMenu>
                                                    </UncontrolledButtonDropdown>
                                                </ButtonGroup>
                                            </div>
                                            <div className="col-auto">
                                                <ButtonGroup vertical>
                                                    <Button color="light">Button</Button>

                                                    <UncontrolledButtonDropdown id="btnGroupVerticalDrop1">
                                                        <DropdownToggle color="light" caret>
                                                            Dropdown
                                                        </DropdownToggle>
                                                        <DropdownMenu>
                                                            <DropdownItem> Dropdown link </DropdownItem>
                                                            <DropdownItem> Dropdown link </DropdownItem>
                                                        </DropdownMenu>
                                                    </UncontrolledButtonDropdown>

                                                    <Button color="light">Button</Button>
                                                    <Button color="light">Button</Button>
                                                </ButtonGroup>
                                            </div>
                                            <div className="col-auto">
                                                <ButtonGroup vertical>
                                                    <Input type="radio" className="btn-check" name="vbtn" id="vbtn-radio1" defaultChecked="" />
                                                    <Button color="secondary" htmlFor="vbtn-radio1" outline>Radio 1</Button>
                                                    <Input type="radio" className="btn-check" name="vbtn" id="vbtn-radio2" />
                                                    <Button color="secondary" htmlFor="vbtn-radio2" outline>Radio 2</Button>
                                                    <Input type="radio" className="btn-check" name="vbtn" id="vbtn-radio3" />
                                                    <Button color="secondary" htmlFor="vbtn-radio3" outline>Radio 3</Button>
                                                </ButtonGroup>
                                            </div>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <VerticalButtonsExample />
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

export default UiButtons;