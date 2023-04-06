import React from 'react';
import { Card, CardBody, Col, Container, Row, } from 'reactstrap';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Images
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar6 from "../../../assets/images/users/avatar-6.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";

import { DefultExample, NestingExample, MediaExample } from './UiMediaobjectCode';

const UiMediaobject = () => {
    document.title="Media object | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">              
                <Container fluid>
                    <BreadCrumb title="Media object" pageTitle="Base UI" />

                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Examples" />

                                <CardBody>

                                    <p className="text-muted mb-4">Use few flex utilities that allow even more flexibility and customization than before.</p>

                                    <div className="live-preview">
                                        <div className="d-flex align-items-start text-muted mb-4">
                                            <div className="flex-shrink-0 me-3">
                                                <img src={avatar2} className="avatar-sm rounded" alt="..." />
                                            </div>

                                            <div className="flex-grow-1">
                                                <h5 className="fs-14">Media heading</h5>
                                                This is some content from a media component. You can replace this with any content and adjust it as needed.
                                            </div>
                                        </div>

                                        <div className="d-flex align-items-start text-muted mb-4">
                                            <div className="flex-grow-1">
                                                <h5 className="fs-14">Media heading</h5>
                                                This is some content from a media component. You can replace this with any content and adjust it as needed.
                                            </div>
                                            <div className="flex-shrink-0 ms-3">
                                                <img src={avatar3} className="avatar-sm rounded" alt="..." />
                                            </div>
                                        </div>

                                        <div className="d-flex align-items-start text-muted">
                                            <div className="flex-shrink-0 me-3">
                                                <img src={avatar2} className="avatar-sm rounded" alt="..." />
                                            </div>
                                            <div className="flex-grow-1">
                                                <h5 className="fs-14">Media heading</h5>
                                                This is some content from a media component. You can replace this with any content and adjust it as needed.
                                            </div>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <DefultExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Nesting Example" />

                                <CardBody>
                                    <p className="text-muted mb-4">Media objects can be infinitely nested, though we suggest you stop at some point. Place nested <code>d-flex align-items-start</code> within the <code>flex-grow-1</code> of a parent media object.</p>
                                    <div className="live-preview">
                                        <div className="d-flex align-items-start text-muted mb-4">
                                            <div className="flex-shrink-0 me-3">
                                                <img src={avatar2} className="avatar-sm rounded" alt="..." />
                                            </div>
                                            <div className="flex-grow-1">
                                                <h5 className="fs-14">Media heading</h5>
                                                This is some content from a media component. You can replace this with any content and adjust it as needed.
                                                <div className="d-flex align-items-start text-muted mt-3">
                                                    <div className="flex-shrink-0 me-3">
                                                        <img src={avatar3} className="avatar-sm rounded" alt="..." />
                                                    </div>
                                                    <div className="flex-grow-1">
                                                        <h5 className="fs-14">Media heading</h5>
                                                        This is some content from a media component. You can replace this with any content and adjust it as needed.
                                                    </div>
                                                </div>
                                            </div>
                                        </div>

                                        <div className="d-flex align-items-start text-muted">
                                            <div className="flex-shrink-0 me-3">
                                                <img src={avatar4} className="avatar-sm rounded" alt="..." />
                                            </div>
                                            <div className="flex-grow-1">
                                                <h5 className="fs-14">Media heading</h5>
                                                This is some content from a media component. You can replace this with any content and adjust it as needed.
                                            </div>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <NestingExample />
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
                                <PreviewCardHeader title="Media Alignment" />

                                <CardBody>

                                    <p className="text-muted mb-4">The images or other media can be aligned top, middle, or bottom. The default is top aligned.</p>

                                    <div className="live-preview">
                                        <div className="d-flex align-items-start text-muted mb-4">
                                            <div className="flex-shrink-0 me-3">
                                                <img src={avatar2} className="avatar-sm rounded" alt="..." />
                                            </div>

                                            <div className="flex-grow-1">
                                                <h5 className="fs-14">Top Aligned media</h5>
                                                <p className="mb-1">Cras sit amet nibh libero, in gravida nulla. Nulla vel metus scelerisque ante sollicitudin. Cras purus odio, vestibulum in vulputate at, tempus viverra turpis. Fusce condimentum nunc ac nisi vulputate fringilla. Donec lacinia congue felis in faucibus.</p>
                                                <p className="mb-0">Donec sed odio dui. Nullam quis risus eget urna mollis ornare vel eu leo. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.</p>
                                            </div>
                                        </div>

                                        <div className="d-flex align-items-center text-muted mb-4">
                                            <div className="flex-shrink-0 me-3">
                                                <img src={avatar6} className="avatar-sm rounded" alt="..." />
                                            </div>

                                            <div className="flex-grow-1">
                                                <h5 className="fs-14">Center Aligned media</h5>
                                                <p className="mb-1">Cras sit amet nibh libero, in gravida nulla. Nulla vel metus scelerisque ante sollicitudin. Cras purus odio, vestibulum in vulputate at, tempus viverra turpis. Fusce condimentum nunc ac nisi vulputate fringilla. Donec lacinia congue felis in faucibus.</p>
                                                <p className="mb-0">Donec sed odio dui. Nullam quis risus eget urna mollis ornare vel eu leo. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.</p>
                                            </div>
                                        </div>

                                        <div className="d-flex align-items-end text-muted">
                                            <div className="flex-shrink-0 me-3">
                                                <img src={avatar8} className="avatar-sm rounded" alt="..." />
                                            </div>

                                            <div className="flex-grow-1">
                                                <h5 className="fs-14">Bottom Aligned media</h5>
                                                <p className="mb-1">Cras sit amet nibh libero, in gravida nulla. Nulla vel metus scelerisque ante sollicitudin. Cras purus odio, vestibulum in vulputate at, tempus viverra turpis. Fusce condimentum nunc ac nisi vulputate fringilla. Donec lacinia congue felis in faucibus.</p>
                                                <p className="mb-0">Donec sed odio dui. Nullam quis risus eget urna mollis ornare vel eu leo. Cum sociis natoque penatibus et magnis dis parturient montes, nascetur ridiculus mus.</p>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <MediaExample />
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

export default UiMediaobject;
