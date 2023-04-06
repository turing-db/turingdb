import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import { Alert, Card, CardBody, Col, Container, Row, UncontrolledAlert } from 'reactstrap';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Content
import UiContent from '../../../Components/Common/UiContent';

//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

// Import Code Components
import { DefaultAlertsExample, BorderlessExample, DismissingExample, LinkColorExample, LiveExample, OutlineAlertsExample, LeftBorderAlertsExample, LabelIconAlertsExample, TopBarAlertsExample, LabelIconArrowAlertsExample, RoundedLabelIconAlertsExample, AdditionalContentAlertsExample, SolidAlertsExample } from './UiAlertsCode';

const UiAlerts = () => {
    document.title="Alerts | Velzon - React Admin & Dashboard Template";
    const [show, setShow] = useState(false);
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
                <Container fluid={true}>
                    <BreadCrumb title="Alerts" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Default Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert</code> class to show a default alert.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>
                                                <Alert color="primary">
                                                    <strong> Hi! </strong> A simple <b>Primary alert</b> —check it out!
                                                </Alert>

                                                <h6>Secondary Alert</h6>
                                                <Alert color="secondary">
                                                    <strong> How are you! </strong> A simple <b>secondary alert</b> —check it out!
                                                </Alert>

                                                <h6>Success Alert</h6>
                                                <Alert color="success">
                                                    <strong> Yey! Everything worked! </strong> A simple <b>success alert</b> —check it out!
                                                </Alert>

                                                <h6>Danger Alert</h6>
                                                <Alert color="danger" className='mb-0'>
                                                    <strong> Something is very wrong! </strong> A simple <b>danger alert</b> —check it out!
                                                </Alert>
                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Alert</h6>
                                                <Alert color="warning">
                                                    <strong> Uh oh, something went wrong </strong> A simple <b>warning alert</b> —check it out!
                                                </Alert>

                                                <h6>Info Alert</h6>
                                                <Alert color="info">
                                                    <strong>Don't forget' it !</strong> A simple <b>info alert</b> —check it out!
                                                </Alert>

                                                <h6>Light Alert</h6>
                                                <Alert color="light">
                                                    <strong>Mind Your Step!</strong> A simple <b>light alert</b> —check it out!
                                                </Alert>

                                                <h6>Dark Alert</h6>
                                                <Alert color="dark" className='mb-0'>
                                                    <strong>Did you know?</strong> A simple <b>dark alert</b> —check it out!
                                                </Alert>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "352px" }}>
                                            <code>
                                                <DefaultAlertsExample />
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
                                <PreviewCardHeader title="Borderless Alerts" />

                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-borderless</code> class to set alert without border.</p>

                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>
                                                <Alert color="primary" className='alert-borderless'>
                                                    <strong> Hi! </strong> A simple <b>Primary alert</b> —check it out!
                                                </Alert>

                                                <h6>Secondary Alert</h6>
                                                <Alert color="secondary" className='alert-borderless'>
                                                    <strong> How are you! </strong> A simple <b>secondary alert</b> —check it out!
                                                </Alert>

                                                <h6>Success Alert</h6>
                                                <Alert color="success" className='alert-borderless'>
                                                    <strong> Yey! Everything worked! </strong> A simple <b>success alert</b> —check it out!
                                                </Alert>

                                                <h6>Danger Alert</h6>
                                                <Alert color="danger" className='alert-borderless mb-xl-0'>
                                                    <strong> Something is very wrong! </strong> A simple danger alert—check it out!
                                                </Alert>

                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Alert</h6>
                                                <Alert color="warning" className='alert-borderless'>
                                                    <strong> Uh oh, something went wrong </strong> A simple <b>warning alert</b> —check it out!
                                                </Alert>

                                                <h6>Info Alert</h6>
                                                <Alert color="info" className='alert-borderless'>
                                                    <strong>Don't forget' it !</strong> A simple <b>info alert</b> —check it out!
                                                </Alert>

                                                <h6>Light Alert</h6>
                                                <Alert color="light" className='alert-borderless bg-light'>
                                                    <strong>Mind Your Step!</strong> A simple <b>light alert</b> —check it out!
                                                </Alert>

                                                <h6>Dark Alert</h6>
                                                <Alert color="dark" className='alert-borderless mb-0'>
                                                    <strong>Did you know?</strong> A simple <b>dark alert</b> —check it out!
                                                </Alert>

                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "352px" }}>
                                            <code>
                                                <BorderlessExample />
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
                                <PreviewCardHeader title="Dismissing Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-dismissible</code> class to add dismissing button to the alert.
                                    </p>
                                    <div className="live-preview">

                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>
                                                <UncontrolledAlert color="primary">
                                                    <strong> Hi! </strong> A simple <b>Dismissible primary Alert </b> — check it out!
                                                </UncontrolledAlert>

                                                <h6>Secondary Alert</h6>
                                                <UncontrolledAlert color="secondary">
                                                    <strong> How are you! </strong> A simple <b>Dismissible secondary alert</b> —check it out!
                                                </UncontrolledAlert>

                                                <h6>Success Alert</h6>
                                                <UncontrolledAlert color="success">
                                                    <strong>Right Way !</strong> A simple <b>Dismissible success alert</b> —check it out!
                                                </UncontrolledAlert>

                                                <h6>Danger Alert</h6>
                                                <UncontrolledAlert color="danger" className="mb-xl-0">
                                                    <strong> Something is very wrong! </strong> A simple <b>Dismissible danger alert</b> —check it out!
                                                </UncontrolledAlert>
                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Alert</h6>
                                                <UncontrolledAlert color="warning">
                                                    <strong>Welcome Back!</strong> A simple <b>Dismissible warning alert</b> —check it out!
                                                </UncontrolledAlert>

                                                <h6>Info Alert</h6>
                                                <UncontrolledAlert color="info">
                                                    <strong>Don't forget' it !</strong> A simple <b>Dismissible info alert</b> —check it out!
                                                </UncontrolledAlert>

                                                <h6>Light Alert</h6>
                                                <UncontrolledAlert color="light">
                                                    <strong>Mind Your Step!</strong> A simple <b>Dismissible light alert</b> —check it out!
                                                </UncontrolledAlert>

                                                <h6>Dark Alert</h6>
                                                <UncontrolledAlert color="dark" className="mb-0">
                                                    <strong>Did you know?</strong> A simple <b>Dismissible dark alert</b> —check it out!
                                                </UncontrolledAlert>

                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "352px" }}>
                                            <code>
                                                <DismissingExample />
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
                                <PreviewCardHeader title="Link Color Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-link</code> class at &lt;a&gt; tag to show matching colored links within the given alert.</p>

                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>

                                                <Alert color="primary">
                                                    A simple Primary alert with <Link to="#" className="alert-link">an example
                                                        link</Link>. Give it a click if you like.
                                                </Alert>

                                                <h6>Secondary Alert</h6>
                                                <Alert color="secondary">
                                                    A simple Secondary alert with <Link to="#" className="alert-link">an example
                                                        link</Link>.  Give it a click if you like.
                                                </Alert>

                                                <h6>Success Alert</h6>
                                                <Alert color="success">
                                                    A simple Success alert with <Link to="#" className="alert-link">an example
                                                        link</Link>. Give it a click if you like.
                                                </Alert>

                                                <h6>Danger Alert</h6>
                                                <Alert color="danger" className="mb-xl-0">
                                                    A simple Danger alert with <Link to="#" className="alert-link">an example
                                                        link</Link>. Give it a click if you like.
                                                </Alert>
                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Alert</h6>
                                                <Alert color="warning">
                                                    A simple Warning alert with <Link to="#" className="alert-link">an example
                                                        link</Link>. Give  it a click if you like.
                                                </Alert>

                                                <h6>Info Alert</h6>
                                                <Alert color="info">
                                                    A simple Info alert with <Link to="#" className="alert-link">an example
                                                        link</Link>. Give it  a click if you like.
                                                </Alert>

                                                <h6>Light Alert</h6>
                                                <Alert color="light">
                                                    A simple Light alert with <Link to="#" className="alert-link">an example
                                                        link</Link>. Give it a click if you like.
                                                </Alert>

                                                <h6>Dark Alert</h6>
                                                <Alert color="dark" className="mb-0">
                                                    A simple Dark alert with <Link to="#" className="alert-link">an example
                                                        link</Link>. Give it a click if you like.
                                                </Alert>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup lang-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <LinkColorExample />
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
                                <PreviewCardHeader title="Live Alert Example" />
                                <CardBody>
                                    <p className="text-muted">Click the Show live alert button to show an alert on button click.</p>
                                    <div className="live-preview">
                                        <div id="liveAlertPlaceholder">
                                            <Alert color="success" isOpen={show} toggle={() => setShow(false)}>
                                                Nice, you triggered this alert message!
                                            </Alert>
                                            <button type="button" className="btn btn-primary" id="liveAlertBtn" onClick={() => setShow(true)}>Show live alert</button>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup language-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <LiveExample />
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
                                <PreviewCardHeader title="Outline Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-outline</code>  class to set an alert with outline.</p>

                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Outline Alert</h6>

                                                <UncontrolledAlert color="primary" className="alert-outline">
                                                    <strong> Hi! </strong> - Outline <b>primary alert</b> example
                                                </UncontrolledAlert>

                                                <h6>Secondary Outline Alert</h6>
                                                <UncontrolledAlert color="secondary" className="alert-outline">
                                                    <strong> How are you! </strong> - Outline <b>secondary alert</b> example
                                                </UncontrolledAlert>

                                                <h6>Success Outline Alert</h6>
                                                <UncontrolledAlert color="success" className="alert-outline">
                                                    <strong> Yey! Everything worked! </strong> - Outline <b>success alert</b> example
                                                </UncontrolledAlert>

                                                <h6>Danger Outline Alert</h6>
                                                <UncontrolledAlert color="danger" className="alert-outline mb-xl-0">
                                                    <strong> Something is very wrong! </strong> - Outline <b>danger alert</b> example
                                                </UncontrolledAlert>

                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Outline Alert</h6>
                                                <UncontrolledAlert color="warning" className="alert-outline">
                                                    <strong> Uh oh, something went wrong! </strong> - Outline <b>warning alert</b> example
                                                </UncontrolledAlert>

                                                <h6>Info Outline Alert</h6>
                                                <UncontrolledAlert color="info" className="alert-outline">
                                                    <strong>Don't forget' it !</strong> - Outline <b>info alert</b> example
                                                </UncontrolledAlert>

                                                <h6>Dark Alert</h6>
                                                <UncontrolledAlert color="dark" className="alert-outline mb-0">
                                                    <strong>Did you know?</strong> - Outline <b>dark alert</b> example
                                                </UncontrolledAlert>

                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup lang-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <OutlineAlertsExample />
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
                                <PreviewCardHeader title="Left Border Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-border-left </code> class to set an alert with the left border & outline.</p>

                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>
                                                <UncontrolledAlert color="primary" className="alert-border-left">
                                                    <i className="ri-user-smile-line me-3 align-middle fs-16"></i><strong>Primary</strong> - Left border alert
                                                </UncontrolledAlert>

                                                <h6>Secondary Alert</h6>
                                                <UncontrolledAlert color="secondary" className="alert-border-left">
                                                    <i className="ri-check-double-line me-3 align-middle fs-16"></i><strong>Secondary</strong>
                                                    - Left border alert
                                                </UncontrolledAlert>

                                                <h6>Success Alert</h6>
                                                <UncontrolledAlert color="success" className="alert-border-left">
                                                    <i className="ri-notification-off-line me-3 align-middle fs-16"></i><strong>Success</strong>
                                                    - Left border alert
                                                </UncontrolledAlert>

                                                <h6>Danger Alert</h6>
                                                <UncontrolledAlert color="danger" className="alert-border-left mb-xl-0">
                                                    <i className="ri-error-warning-line me-3 align-middle fs-16"></i><strong>Danger</strong>
                                                    - Left border alert
                                                </UncontrolledAlert>

                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Alert</h6>
                                                <UncontrolledAlert color="warning" className="alert-border-left">
                                                    <i className="ri-alert-line me-3 align-middle fs-16"></i><strong>Warning</strong>
                                                    - Left border alert
                                                </UncontrolledAlert>

                                                <h6>Info Alert</h6>
                                                <UncontrolledAlert color="info" className="alert-border-left">
                                                    <i className="ri-airplay-line me-3 align-middle fs-16"></i><strong>Info</strong>
                                                    - Left border alert
                                                </UncontrolledAlert>

                                                <h6>Light Alert</h6>
                                                <UncontrolledAlert color="light" className="alert-border-left">
                                                    <i className="ri-mail-line me-3 align-middle fs-16"></i><strong>Light</strong>
                                                    - Left border alert
                                                </UncontrolledAlert>

                                                <h6>Dark Alert</h6>
                                                <UncontrolledAlert color="dark" className="alert-border-left mb-0">
                                                    <i className="ri-refresh-line me-3  align-middle fs-16"></i><strong>Dark</strong>
                                                    - Left border alert
                                                </UncontrolledAlert>

                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup lang-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <LeftBorderAlertsExample />
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
                                <PreviewCardHeader title="Label Icon Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-label-icon</code> class to set an alert with a label icon.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>
                                                <UncontrolledAlert color="primary" className="alert-solid alert-label-icon" closeClassName="btn-close-white">
                                                    <i className="ri-user-smile-line label-icon"></i><strong>Primary</strong> -Label icon alert
                                                </UncontrolledAlert>

                                                <h6>Secondary Alert</h6>
                                                <UncontrolledAlert color="secondary" className="alert-solid alert-label-icon" closeClassName="btn-close-white">
                                                    <i className="ri-check-double-line label-icon"></i><strong>Secondary</strong>
                                                    - Label icon
                                                    alert
                                                </UncontrolledAlert>

                                                <h6>Success Alert</h6>
                                                <UncontrolledAlert color="success" className="alert-solid alert-label-icon" closeClassName="btn-close-white">
                                                    <i className="ri-notification-off-line label-icon"></i><strong>Success</strong>
                                                    - Label
                                                    icon alert
                                                </UncontrolledAlert>

                                                <h6>Danger Alert</h6>
                                                <UncontrolledAlert color="danger" className="alert-solid alert-label-icon mb-xl-0" closeClassName="btn-close-white">
                                                    <i className="ri-error-warning-line label-icon"></i><strong>Danger</strong>
                                                    - Label
                                                    icon alert
                                                </UncontrolledAlert>

                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Alert</h6>
                                                <UncontrolledAlert color="warning" className="alert-solid alert-label-icon" closeClassName="btn-close-white">
                                                    <i className="ri-alert-line label-icon"></i><strong>warning</strong> - Label
                                                    icon alert
                                                </UncontrolledAlert>

                                                <h6>Info Alert</h6>
                                                <UncontrolledAlert color="info" className="alert-solid alert-label-icon" closeClassName="btn-close-white">
                                                    <i className="ri-airplay-line label-icon"></i><strong>Info</strong> - Label
                                                    icon alert
                                                </UncontrolledAlert>

                                                <h6>Light Alert</h6>
                                                <UncontrolledAlert color="light" className="alert-solid alert-label-icon" closeClassName="btn-close-white">
                                                    <i className="ri-mail-line label-icon"></i><strong>Light</strong> -
                                                    Label icon alert
                                                </UncontrolledAlert>

                                                <h6>Dark Alert</h6>
                                                <UncontrolledAlert color="dark" className="alert-solid alert-label-icon mb-0" closeClassName="btn-close-white">
                                                    <i className="ri-refresh-line label-icon"></i><strong>Dark</strong> -
                                                    Label icon alert
                                                </UncontrolledAlert>

                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup lang-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <LabelIconAlertsExample />
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
                                <PreviewCardHeader title="Additional Content Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-additional</code> class and Use the <code>alert-</code> class to HTML elements like headings, paragraphs, dividers etc.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>

                                                <UncontrolledAlert color="primary" className="alert-additional">
                                                    <div className="alert-body">
                                                        <div className="d-flex">
                                                            <div className="flex-shrink-0 me-3">
                                                                <i className="ri-error-warning-line fs-16 align-middle"></i>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <h5 className="alert-heading">Well done !</h5>
                                                                <p className="mb-0">Aww yeah, you successfully read this important alert message. </p>
                                                            </div>
                                                        </div>
                                                    </div>
                                                    <div className="alert-content">
                                                        <p className="mb-0">Whenever you need to, be sure to use margin
                                                            utilities to keep things nice and tidy.</p>
                                                    </div>
                                                </UncontrolledAlert>

                                                <h6>Danger Alert</h6>
                                                <UncontrolledAlert color="danger" className="alert-additional mb-xl-0">
                                                    <div className="alert-body">
                                                        <div className="d-flex">
                                                            <div className="flex-shrink-0 me-3">
                                                                <i className="ri-alert-line fs-16 align-middle"></i>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <h5 className="alert-heading">Something is very wrong!</h5>
                                                                <p className="mb-0">Change a few things up and try submitting again.</p>
                                                            </div>
                                                        </div>
                                                    </div>
                                                    <div className="alert-content">
                                                        <p className="mb-0">Whenever you need to, be sure to use margin
                                                            utilities to keep things nice and tidy.</p>
                                                    </div>
                                                </UncontrolledAlert>
                                            </Col>

                                            <Col xl={6}>

                                                <h6>Success Alert</h6>
                                                <UncontrolledAlert color="success" className="alert-additional">
                                                    <div className="alert-body">
                                                        <div className="d-flex">
                                                            <div className="flex-shrink-0 me-3">
                                                                <i className="ri-notification-off-line fs-16 align-middle"></i>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <h5 className="alert-heading">Yey! Everything worked!</h5>
                                                                <p className="mb-0">This alert needs your attention, but it"s not super important.</p>
                                                            </div>
                                                        </div>
                                                    </div>
                                                    <div className="alert-content">
                                                        <p className="mb-0">Whenever you need to, be sure to use margin
                                                            utilities to keep things nice and tidy.</p>
                                                    </div>
                                                </UncontrolledAlert>

                                                <h6>Warning Alert</h6>
                                                <UncontrolledAlert color="warning" className="alert-additional mb-0">
                                                    <div className="alert-body">
                                                        <div className="d-flex">
                                                            <div className="flex-shrink-0 me-3">
                                                                <i className="ri-alert-line fs-16 align-middle"></i>
                                                            </div>
                                                            <div className="flex-grow-1">
                                                                <h5 className="alert-heading">Uh oh, something went wrong!</h5>
                                                                <p className="mb-0">Better check yourself, you're not looking too good.</p>
                                                            </div>
                                                        </div>
                                                    </div>
                                                    <div className="alert-content">
                                                        <p className="mb-0">Whenever you need to, be sure to use margin
                                                            utilities to keep things nice and tidy.</p>
                                                    </div>
                                                </UncontrolledAlert>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup lang-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <AdditionalContentAlertsExample />
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
                                <PreviewCardHeader title="Top Border with Outline Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-top-border</code> class to set an alert with the top border and outline.</p>
                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>
                                                <UncontrolledAlert color="primary" className="alert-top-border">
                                                    <i className="ri-user-smile-line me-3 align-middle fs-16 text-primary"></i><strong>Primary</strong>
                                                    - Top border alert
                                                </UncontrolledAlert>

                                                <h6>Secondary Alert</h6>
                                                <UncontrolledAlert color="secondary" className="alert-top-border">
                                                    <i className="ri-check-double-line me-3 align-middle fs-16 text-secondary"></i><strong>Secondary</strong>
                                                    - Top border alert
                                                </UncontrolledAlert>

                                                <h6>Success Alert</h6>
                                                <UncontrolledAlert color="success" className="alert-top-border">
                                                    <i className="ri-notification-off-line me-3 align-middle fs-16 text-success"></i><strong>Success</strong>
                                                    - Top border alert
                                                </UncontrolledAlert>

                                                <h6>Danger Alert</h6>
                                                <UncontrolledAlert color="danger" className="alert-top-border mb-xl-0">
                                                    <i className="ri-error-warning-line me-3 align-middle fs-16 text-danger "></i><strong>Danger</strong>
                                                    - Top border alert
                                                </UncontrolledAlert>

                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Alert</h6>
                                                <UncontrolledAlert color="warning" className="alert-top-border">
                                                    <i className="ri-alert-line me-3 align-middle fs-16 text-warning"></i><strong>Warning</strong>
                                                    - Top border alert
                                                </UncontrolledAlert>

                                                <h6>Info Alert</h6>
                                                <UncontrolledAlert color="info" className="alert-top-border">
                                                    <i className="ri-airplay-line me-3 align-middle fs-16 text-info"></i><strong>Info</strong>
                                                    - Top border alert
                                                </UncontrolledAlert>

                                                <h6>Light Alert</h6>
                                                <UncontrolledAlert color="light" className="alert-top-border">
                                                    <i className="ri-mail-line me-3 align-middle fs-16 text-dark"></i><strong>Light</strong>
                                                    - Top border alert
                                                </UncontrolledAlert>

                                                <h6>Dark Alert</h6>
                                                <UncontrolledAlert color="dark" className="alert-top-border mb-0">
                                                    <i className="ri-refresh-line me-3 align-middle fs-16 text-dark"></i><strong>Dark</strong>
                                                    - Top border alert
                                                </UncontrolledAlert>

                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup lang-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <TopBarAlertsExample />
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
                                <PreviewCardHeader title="Label Icon Arrow Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-label-icon label-arrow</code> class to show an alert with label icon and arrow.</p>
                                    <div className="live-preview">

                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>
                                                <UncontrolledAlert color="primary" className="alert-label-icon label-arrow ">
                                                    <i className="ri-user-smile-line label-icon"></i><strong>Primary</strong> -
                                                    Label icon arrow  alert
                                                </UncontrolledAlert>

                                                <h6>Secondary Alert</h6>
                                                <UncontrolledAlert color="secondary" className="alert-label-icon label-arrow ">
                                                    <i className="ri-check-double-line label-icon"></i><strong>Secondary</strong> -
                                                    Label icon
                                                    arrow alert
                                                </UncontrolledAlert>

                                                <h6>Success Alert</h6>
                                                <UncontrolledAlert color="success" className="alert-label-icon label-arrow ">
                                                    <i className="ri-notification-off-line label-icon"></i><strong>Success</strong>
                                                    - Label
                                                    icon arrow alert
                                                </UncontrolledAlert>

                                                <h6>Danger Alert</h6>
                                                <UncontrolledAlert color="danger" className="alert-label-icon label-arrow mb-xl-0">
                                                    <i className="ri-error-warning-line label-icon"></i><strong>Danger</strong>
                                                    - Label
                                                    icon arrow alert
                                                </UncontrolledAlert>

                                            </Col>

                                            <Col xl={6}>

                                                <h6>Warning Alert</h6>
                                                <UncontrolledAlert color="warning" className="alert-label-icon label-arrow ">
                                                    <i className="ri-alert-line label-icon"></i><strong>Warning</strong> -
                                                    Label icon arrow alert
                                                </UncontrolledAlert>

                                                <h6>info Alert</h6>
                                                <UncontrolledAlert color="info" className="alert-label-icon label-arrow ">
                                                    <i className="ri-airplay-line label-icon"></i><strong>Info</strong> -
                                                    Label icon arrow alert
                                                </UncontrolledAlert>

                                                <h6>Light Alert</h6>
                                                <UncontrolledAlert color="light" className="alert-label-icon label-arrow ">
                                                    <i className="ri-mail-line label-icon"></i><strong>Light</strong>
                                                    - Label icon arrow alert
                                                </UncontrolledAlert>

                                                <h6>Dark Alert</h6>
                                                <UncontrolledAlert color="dark" className="alert-label-icon label-arrow mb-0">
                                                    <i className="ri-refresh-line label-icon"></i><strong>Dark</strong>
                                                    - Label icon arrow alert
                                                </UncontrolledAlert>

                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup lang-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <LabelIconArrowAlertsExample />
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
                                <PreviewCardHeader title="Rounded Label Icon Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-label-icon rounded-label</code> class to set an alert with a rounded label icon.</p>
                                    <div className="live-preview">

                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Alert</h6>
                                                <UncontrolledAlert color="primary" className="alert-label-icon rounded-label">
                                                    <i className="ri-user-smile-line label-icon"></i><strong>Primary</strong> -
                                                    Rounded label alert
                                                </UncontrolledAlert>

                                                <h6>Secondary Alert</h6>
                                                <UncontrolledAlert color="secondary" className="alert-label-icon rounded-label">
                                                    <i className="ri-check-double-line label-icon"></i><strong>Secondary</strong>
                                                    - Rounded
                                                    label alert
                                                </UncontrolledAlert>

                                                <h6>Success Alert</h6>
                                                <UncontrolledAlert color="success" className="alert-label-icon rounded-label">
                                                    <i className="ri-notification-off-line label-icon"></i><strong>Success</strong>
                                                    - Rounded
                                                    label alert
                                                </UncontrolledAlert>

                                                <h6>Danger Alert</h6>
                                                <UncontrolledAlert color="danger" className="alert-label-icon rounded-label">
                                                    <i className="ri-error-warning-line label-icon"></i><strong>Danger</strong>
                                                    - Rounded
                                                    label alert
                                                </UncontrolledAlert>

                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Alert</h6>
                                                <UncontrolledAlert color="warning" className="alert-label-icon rounded-label">
                                                    <i className="ri-alert-line label-icon"></i><strong>Warning</strong> - Rounded
                                                    label alert
                                                </UncontrolledAlert>

                                                <h6>Info Alert</h6>
                                                <UncontrolledAlert color="info" className="alert-label-icon rounded-label">
                                                    <i className="ri-airplay-line label-icon"></i><strong>Info</strong> -
                                                    Rounded label
                                                    alert
                                                </UncontrolledAlert>

                                                <h6>Light Alert</h6>
                                                <UncontrolledAlert color="light" className="alert-label-icon rounded-label">
                                                    <i className="ri-mail-line label-icon"></i><strong>Light</strong> -
                                                    Rounded label
                                                    alert
                                                </UncontrolledAlert>

                                                <h6>Dark Alert</h6>
                                                <UncontrolledAlert color="dark" className="alert-label-icon rounded-label">
                                                    <i className="ri-refresh-line label-icon"></i><strong>Dark</strong> -
                                                    Rounded label
                                                    alert
                                                </UncontrolledAlert>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup lang-jsx" style={{ maxHeight: "260px" }}>
                                            <code>
                                                <RoundedLabelIconAlertsExample />
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
                                <PreviewCardHeader title="Solid Alerts" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>alert-solid</code> class to set an alert with solid style.</p>

                                    <div className="live-preview">
                                        <Row>
                                            <Col xl={6}>
                                                <h6>Primary Solid Alert</h6>
                                                <Alert color="primary" className="alert-solid">
                                                    <strong>Hi!</strong> - Solid <b>primary alert</b> example
                                                </Alert>

                                                <h6>Secondary Solid Alert</h6>
                                                <Alert color="secondary" className="alert-solid">
                                                    <strong>How are you!</strong> - Solid <b>secondary alert</b> example
                                                </Alert>

                                                <h6>Success Solid Alert</h6>
                                                <Alert color="success" className="alert-solid">
                                                    <strong>Yey! Everything worked! </strong> - Solid <b>success alert</b> example
                                                </Alert>

                                                <h6>Danger Solid Alert</h6>
                                                <Alert color="danger" className="alert-solid mb-xl-0">
                                                    <strong>Something is very wrong!</strong> - Solid <b>danger alert</b> example
                                                </Alert>

                                            </Col>

                                            <Col xl={6}>
                                                <h6>Warning Solid Alert</h6>
                                                <Alert color="warning" className="alert-solid">
                                                    <strong>Uh oh, something went wrong!</strong> - Solid <b>warning alert</b> example
                                                </Alert>

                                                <h6>Info Solid Alert</h6>
                                                <Alert color="info" className="alert-solid">
                                                    <strong>Don't forget' it !</strong> - Solid <b>info alert</b> example
                                                </Alert>

                                                <h6>Light Solid Alert</h6>
                                                <Alert color="light" className="alert-solid">
                                                    <strong>Mind Your Step!</strong> - Solid <b>secondary alert</b> example
                                                </Alert>

                                                <h6>Dark Solid Alert</h6>
                                                <Alert color="dark" className="alert-solid mb-0">
                                                    <strong>Did you know?</strong> - Solid <b>dark alert</b> example
                                                </Alert>

                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "260px" }}>
                                            <code>
                                                <SolidAlertsExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div>
        </React.Fragment >
    );
};

export default UiAlerts;
