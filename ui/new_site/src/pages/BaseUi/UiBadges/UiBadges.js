import React from 'react';
import { Badge, Button, Card, CardBody, Col, Container, Row, } from 'reactstrap';

import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Content
import UiContent from '../../../Components/Common/UiContent';

//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

import {
    DefaultBadgesExample,
    RoundedPillBadgesExample,
    LabelBadgesExample,
    HTMLBadgesExample,
    SoftBadgesExample,
    OutlineBadgesExample,
    RoundSoftBadgesExample,
    SoftBorderBadgesExample,
    OutlinePillBadgesExample,
    ButtonPositionBadgesExample,
    ButtonBadgesExample,
    GradientBadgesExample
} from './UiBadgesCode';

const UiBadges = () => {
    document.title = "Badges | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
                <Container fluid>
                    <BreadCrumb title="Badges" pageTitle="Base UI" />
                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Default Badges" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>badge</code> class to set a default badge.</p>
                                    <div className="live-preview">

                                        <div className="d-flex flex-wrap gap-2">
                                            <Badge color="primary"> Primary </Badge>
                                            <Badge color="secondary"> Secondary </Badge>
                                            <Badge color="success"> Success </Badge>
                                            <Badge color="info"> Info </Badge>
                                            <Badge color="warning"> Warning </Badge>
                                            <Badge color="danger"> Danger </Badge>
                                            <Badge color="dark"> Dark </Badge>
                                            <Badge color="light" className="text-dark"> Light </Badge>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <DefaultBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Soft Badges" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>badge-soft-</code> class with the below-mentioned variation to create a softer badge.
                                    </p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <span className="badge badge-soft-primary">Primary</span>
                                            <span className="badge badge-soft-secondary">Secondary</span>
                                            <span className="badge badge-soft-success">Success</span>
                                            <span className="badge badge-soft-info">Info</span>
                                            <span className="badge badge-soft-warning">Warning</span>
                                            <span className="badge badge-soft-danger">Danger</span>
                                            <span className="badge badge-soft-dark">Dark</span>
                                            <span className="badge badge-soft-light text-dark">Light</span>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <SoftBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Outline Badges" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>badge-outline-</code> class with the below-mentioned variation to create a badge with the outline.
                                    </p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <span className="badge badge-outline-primary">Primary</span>
                                            <span className="badge badge-outline-secondary">Secondary</span>
                                            <span className="badge badge-outline-success">Success</span>
                                            <span className="badge badge-outline-info">Info</span>
                                            <span className="badge badge-outline-warning">Warning</span>
                                            <span className="badge badge-outline-danger">Danger</span>
                                            <span className="badge badge-outline-dark">Dark</span>
                                            <span className="badge badge-outline-light text-dark">Light</span>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <OutlineBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Rounded Pill Badges" />
                                <CardBody>
                                    <p className="text-muted">Use the <code>rounded-pill</code> class to make badges more rounded
                                        with a larger border-radius.</p>

                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Badge color="primary" pill> Primary </Badge>
                                            <Badge color="secondary" pill> Secondary </Badge>
                                            <Badge color="success" pill> Success </Badge>
                                            <Badge color="info" pill> Info </Badge>
                                            <Badge color="warning" pill> Warning </Badge>
                                            <Badge color="danger" pill> Danger </Badge>
                                            <Badge color="dark" pill> Dark </Badge>
                                            <Badge color="light" className="text-dark" pill> Light </Badge>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <RoundedPillBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Rounded Pill Badges with soft effect" />
                                <CardBody>

                                    <p className="text-muted">Use the <code>rounded-pill badge-soft-</code> class with the below-mentioned variation
                                        to create a badge more rounded with a soft background.</p>
                                    <div className="live-preview">

                                        <div className="d-flex flex-wrap gap-2">
                                            <span className="badge rounded-pill badge-soft-primary">Primary</span>
                                            <span className="badge rounded-pill badge-soft-secondary">Secondary</span>
                                            <span className="badge rounded-pill badge-soft-success">Success</span>
                                            <span className="badge rounded-pill badge-soft-info">Info</span>
                                            <span className="badge rounded-pill badge-soft-warning">Warning</span>
                                            <span className="badge rounded-pill badge-soft-danger">Danger</span>
                                            <span className="badge rounded-pill badge-soft-dark">Dark</span>
                                            <span className="badge rounded-pill badge-soft-light text-dark">Light</span>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <RoundSoftBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Soft Border Badges" />
                                <CardBody>

                                    <p className="text-muted">
                                        Use the <code>badge-border</code> and <code>badge-soft-</code> with below mentioned modifier classes to make badges with border & soft backgorund.
                                    </p>
                                    <div className="live-preview">

                                        <div className="d-flex flex-wrap gap-2">
                                            <span className="badge badge-soft-primary badge-border">Primary</span>
                                            <span className="badge badge-soft-secondary badge-border">Secondary</span>
                                            <span className="badge badge-soft-success badge-border">Success</span>
                                            <span className="badge badge-soft-danger badge-border">Danger</span>
                                            <span className="badge badge-soft-warning badge-border">Warning</span>
                                            <span className="badge badge-soft-info badge-border">Info</span>
                                            <span className="badge badge-soft-dark badge-border">Dark</span>
                                            <span className="badge badge-soft-light badge-border text-dark">Light</span>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <SoftBorderBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Outline Pill Badges" />
                                <CardBody>
                                    <p className="text-muted">
                                        Use the <code>rounded-pill badge-outline-</code> class with the below-mentioned variation
                                        to create a outline Pill badge.
                                    </p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <span className="badge rounded-pill badge-outline-primary">Primary</span>
                                            <span className="badge rounded-pill badge-outline-secondary">Secondary</span>
                                            <span className="badge rounded-pill badge-outline-success">Success</span>
                                            <span className="badge rounded-pill badge-outline-info">Info</span>
                                            <span className="badge rounded-pill badge-outline-warning">Warning</span>
                                            <span className="badge rounded-pill badge-outline-danger">Danger</span>
                                            <span className="badge rounded-pill badge-outline-dark">Dark</span>
                                            <span className="badge rounded-pill badge-outline-light text-dark">Light</span>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <OutlinePillBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Label Badges" />

                                <CardBody>
                                    <p className="text-muted">
                                        Use the <code>badge-label</code> class to create a badge with the label.
                                    </p>

                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Badge color="primary" className="badge-label"> <i className="mdi mdi-circle-medium"></i> Primary </Badge>
                                            <Badge color="secondary" className="badge-label"> <i className="mdi mdi-circle-medium"></i> Secondary </Badge>
                                            <Badge color="success" className="badge-label"> <i className="mdi mdi-circle-medium"></i> Success </Badge>
                                            <Badge color="danger" className="badge-label"> <i className="mdi mdi-circle-medium"></i> Danger </Badge>
                                            <Badge color="warning" className="badge-label"> <i className="mdi mdi-circle-medium"></i> Warning </Badge>
                                            <Badge color="info" className="badge-label"> <i className="mdi mdi-circle-medium"></i> Info </Badge>
                                            <Badge color="dark" className="badge-label"> <i className="mdi mdi-circle-medium"></i> Dark </Badge>
                                            <Badge color="light" className="badge-label text-dark"> <i className="mdi mdi-circle-medium"></i> Light </Badge>
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <LabelBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col xxl={12}>
                            <Card>
                                <PreviewCardHeader title="Gradient Badges" />

                                <CardBody>
                                    <p className="text-muted">
                                        Use the <code>badge-gradient-*</code> class to create a gradient styled badge.
                                    </p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-2">
                                            <Badge className="badge-gradient-primary">Primary</Badge>
                                            <Badge className="badge-gradient-secondary">Secondary</Badge>
                                            <Badge className="badge-gradient-success">Success</Badge>
                                            <Badge className="badge-gradient-danger">Danger</Badge>
                                            <Badge className="badge-gradient-warning">Warning</Badge>
                                            <Badge className="badge-gradient-info">Info</Badge>
                                            <Badge className="badge-gradient-dark">Dark</Badge>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <GradientBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Button Position Badges" />

                                <CardBody>
                                    <p className="text-muted">Use the below utilities to modify a badge and position it in the corner of a link or button.</p>
                                    <div className="live-preview">
                                        <div className="d-flex flex-wrap gap-3">
                                            <Button color="primary" className="position-relative">
                                                Mails <Badge pill color="success"
                                                    className="position-absolute top-0 start-100 translate-middle">+99
                                                    <span className="visually-hidden">unread messages</span></Badge>
                                            </Button>

                                            <Button color="light" className="position-relative">
                                                Alerts <span
                                                    className="position-absolute top-0 start-100 translate-middle badge border border-light rounded-circle bg-danger p-1"><span
                                                        className="visually-hidden">unread messages</span></span>
                                            </Button>

                                            <Button type="button" color="primary" className="position-relative p-0 avatar-xs rounded">
                                                <span className="avatar-title bg-transparent">
                                                    <i className="bx bxs-envelope"></i>
                                                </span>
                                                <span
                                                    className="position-absolute top-0 start-100 translate-middle badge border border-light rounded-circle bg-danger p-1"><span
                                                        className="visually-hidden">unread messages</span></span>
                                            </Button>

                                            <Button color="light" className="position-relative p-0 avatar-xs rounded-circle">
                                                <span className="avatar-title bg-transparent text-reset">
                                                    <i className="bx bxs-bell"></i>
                                                </span>
                                            </Button>

                                            <Button color="light" className="position-relative p-0 avatar-xs rounded-circle">
                                                <span className="avatar-title bg-transparent text-reset">
                                                    <i className="bx bx-menu"></i>
                                                </span>
                                                <span
                                                    className="position-absolute top-0 start-100 translate-middle badge border border-light rounded-circle bg-success p-1"><span
                                                        className="visually-hidden">unread messages</span></span>
                                            </Button>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <ButtonPositionBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>

                            <Card>
                                <PreviewCardHeader title="Badges With Button" />
                                <CardBody>
                                    <p className="text-muted">Badges can be used as part of buttons to provide a counter.</p>

                                    <div className="live-preview">

                                        <div className="d-flex flex-wrap gap-2">

                                            <Button color="primary">
                                                Notifications <Badge color="success" className="ms-1">4</Badge>
                                            </Button>
                                            <Button color="success">
                                                Messages <Badge color="danger" className="ms-1">2</Badge>
                                            </Button>
                                            <Button color="secondary" outline>
                                                Draft <Badge color="success" className="ms-1">2</Badge>
                                            </Button>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <ButtonBadgesExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Badges with Heading" />
                                <CardBody>
                                    <p className="text-muted">Example of the badge used in the HTML Heading.</p>
                                    <div className="live-preview">
                                        <h1>Example heading  <Badge color="secondary"> New </Badge> </h1>
                                        <h2>Example heading  <Badge color="secondary"> New </Badge> </h2>
                                        <h3>Example heading  <Badge color="secondary"> New </Badge> </h3>
                                        <h4>Example heading  <Badge color="secondary"> New </Badge> </h4>
                                        <h5>Example heading  <Badge color="secondary"> New </Badge> </h5>
                                        <h6 className="mb-0">Example heading  <Badge color="secondary"> New </Badge> </h6>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "200px" }}>
                                            <code>
                                                <HTMLBadgesExample />
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

export default UiBadges;
