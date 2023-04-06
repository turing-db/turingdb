import React from 'react';
import { Card, CardBody, Col, Container, Row, UncontrolledTooltip, } from 'reactstrap';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';

// Import Images
import avatar2 from "../../../assets/images/users/avatar-2.jpg";
import avatar3 from "../../../assets/images/users/avatar-3.jpg";
import avatar4 from "../../../assets/images/users/avatar-4.jpg";
import avatar5 from "../../../assets/images/users/avatar-5.jpg";
import avatar8 from "../../../assets/images/users/avatar-8.jpg";
import avatar10 from "../../../assets/images/users/avatar-10.jpg";


import img2 from "../../../assets/images/small/img-2.jpg";
import img3 from "../../../assets/images/small/img-3.jpg";
import img4 from "../../../assets/images/small/img-4.jpg";
import img5 from "../../../assets/images/small/img-5.jpg";
import { Link } from 'react-router-dom';

import { ImgRoundedCircleExample, ImgThumbnailsExample, ImgSizesExample, AvatarExample, AvatarGroupExample, FiguresExample, ResponsiveExample } from './UiImagesCode';

const UiImages = () => {
    document.title="Images | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Images" pageTitle="Base UI" />
                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Image Rounded & Circle" />
                                <CardBody>
                                    <p className="text-muted">Use
                                        <code> rounded</code> class and <code>rounded-circle</code> class to show an image with a round border and rounded shape respectively.
                                    </p>

                                    <div className="live-preview">

                                        <Row className="align-items-center">
                                            <Col className="col-6">
                                                <img className="rounded" alt="200x200" width="200" src={img4} />
                                            </Col>
                                            <Col className="col-6">
                                                <div className="mt-4 mt-md-0">
                                                    <img className="rounded-circle avatar-xl" alt="200x200" src={avatar4} />
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ImgRoundedCircleExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>

                            <Card>
                                <PreviewCardHeader title="Image Thumbnails" />
                                <CardBody>

                                    <p className="text-muted">Use <code>img-thumbnail</code> class to give an image
                                        rounded 1px border appearance.</p>

                                    <div className="live-preview">

                                        <Row>
                                            <Col className="col-6">
                                                <img className="img-thumbnail" alt="200x200" width="200" src={img3} />
                                            </Col>
                                            <Col className="col-6">
                                                <div className="mt-4 mt-md-0">
                                                    <img className="img-thumbnail rounded-circle avatar-xl" alt="200x200" src={avatar3} />
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ImgThumbnailsExample />
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
                                <PreviewCardHeader title="Image Sizes" />
                                <CardBody>
                                    <p className="text-muted mb-4">Use <code>avatar-xxs</code>, <code>avatar-xs</code>, <code>avatar-sm</code>, <code>avatar-md</code>, <code>avatar-lg</code>, <code>avatar-xl</code> class for different image sizes.</p>

                                    <div className="live-preview">

                                        <Row>
                                            <Col md={12}>
                                                <Row className="g-3">
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar2} alt="" className="rounded avatar-xxs" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-xxs</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar10} alt="" className="rounded avatar-xs" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-xs</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar3} alt="" className="rounded avatar-sm" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-sm</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar4} alt="" className="rounded avatar-md" />
                                                            <p className="mt-2  mb-lg-0"><code>avatar-md</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar5} alt="" className="rounded avatar-lg" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-lg</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar8} alt="" className="rounded avatar-xl" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-xl</code></p>
                                                        </div>
                                                    </Col>
                                                </Row>
                                            </Col>

                                            <Col md={12}>
                                                <Row className="g-3 mt-5">
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar2} alt="" className="rounded-circle avatar-xxs" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-xxs</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar10} alt="" className="rounded-circle avatar-xs" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-xs</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar3} alt="" className="rounded-circle avatar-sm" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-sm</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar4} alt="" className="rounded-circle avatar-md" />
                                                            <p className="mt-2  mb-lg-0"><code>avatar-md</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar5} alt="" className="rounded-circle avatar-lg" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-lg</code></p>
                                                        </div>
                                                    </Col>
                                                    <Col xxl={2} md={4} className="col-6">
                                                        <div>
                                                            <img src={avatar8} alt="" className="rounded-circle avatar-xl" />
                                                            <p className="mt-2 mb-lg-0"><code>avatar-xl</code></p>
                                                        </div>
                                                    </Col>
                                                </Row>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ImgSizesExample />
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
                                <PreviewCardHeader title="Avatar With Content" />

                                <CardBody>
                                    <p className="text-muted">Use <code>avatar-xxs, avatar-xs,avatar-sm,avatar-md,avatar-lg,avatar-xl</code> class to set different avatar content.</p>

                                    <div className="live-preview">

                                        <Row className="g-3">
                                            <Col xxl={2} md={4} className="col-6">
                                                <div className="avatar-xxs mt-3">
                                                    <div className="avatar-title rounded bg-soft-primary text-primary fs-10">
                                                        XXS
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col xxl={2} md={4} className="col-6">
                                                <div className="avatar-xs mt-3">
                                                    <div className="avatar-title rounded bg-soft-secondary text-secondary">
                                                        XS
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col xxl={2} md={4} className="col-6">
                                                <div className="avatar-sm mt-3">
                                                    <div className="avatar-title rounded bg-soft-success text-success fs-14">
                                                        SM
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col xxl={2} md={4} className="col-6">
                                                <div className="avatar-md mt-3">
                                                    <div className="avatar-title rounded bg-soft-info text-info fs-16">
                                                        MD
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col xxl={2} md={4} className="col-6">
                                                <div className="avatar-lg mt-3">
                                                    <div className="avatar-title rounded bg-soft-warning text-warning fs-20">
                                                        LG
                                                    </div>
                                                </div>
                                            </Col>
                                            <Col xxl={2} md={4} className="col-6">
                                                <div className="avatar-xl mt-3">
                                                    <div className="avatar-title rounded bg-soft-danger text-danger fs-22">
                                                        XL
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <AvatarExample />
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
                                <PreviewCardHeader title="Avatar Group" />

                                <CardBody>
                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={6}>
                                                <div className="mt-lg-0 mt-3">
                                                    <p className="text-muted">Use <code>avatar-group</code> class to show avatar images with the group.</p>
                                                    <div className="avatar-group">
                                                        <div className="avatar-group-item">
                                                            <img src={avatar4} alt="" className="rounded-circle avatar-sm" />
                                                        </div>
                                                        <div className="avatar-group-item">
                                                            <img src={avatar5} alt="" className="rounded-circle avatar-sm" />
                                                        </div>
                                                        <div className="avatar-group-item">
                                                            <div className="avatar-sm">
                                                                <div className="avatar-title rounded-circle bg-light text-primary">
                                                                    A
                                                                </div>
                                                            </div>
                                                        </div>
                                                        <div className="avatar-group-item">
                                                            <img src={avatar2} alt="" className="rounded-circle avatar-sm" />
                                                        </div>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col lg={6}>
                                                <div className="mt-lg-0 mt-3">
                                                    <p className="text-muted">Use <code>avatar-group</code> class with <code>data-bs-toggle="tooltip"</code> to show avatar group images with tooltip.</p>
                                                    <div className="avatar-group">
                                                        <Link to="#" className="avatar-group-item" id="img1">
                                                            <img src={avatar4} alt="" className="rounded-circle avatar-sm" />
                                                        </Link>
                                                        <UncontrolledTooltip placement="top" target="img1" > Christi </UncontrolledTooltip>
                                                        <Link to="#" className="avatar-group-item" id="img2">
                                                            <img src={avatar3} alt="" className="rounded-circle avatar-sm" />
                                                        </Link>
                                                        <UncontrolledTooltip placement="top" target="img2" > Frank Hook </UncontrolledTooltip>
                                                        <Link to="#" className="avatar-group-item" id="img3">
                                                            <div className="avatar-sm">
                                                                <div className="avatar-title rounded-circle bg-light text-primary">
                                                                    C
                                                                </div>
                                                            </div>
                                                        </Link>
                                                        <UncontrolledTooltip placement="top" target="img3" > Christi </UncontrolledTooltip>
                                                        <Link to="#" className="avatar-group-item" id="img4">
                                                            <div className="avatar-sm">
                                                                <div className="avatar-title rounded-circle">
                                                                    2+
                                                                </div>
                                                            </div>
                                                        </Link>
                                                        <UncontrolledTooltip placement="top" target="img4" > More </UncontrolledTooltip>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <AvatarGroupExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={8}>
                            <Card>
                                <PreviewCardHeader title="Figures" />
                                <CardBody>
                                    <p className="card-title-desc text-muted">Use the included <code>figure</code>, <code>figure-img</code> and <code>figure-caption</code> classes to provide some baseline styles for the HTML5 <code>&lt;figure&gt;</code> and <code>&lt;figcaption&gt;</code> elements.</p>

                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <Col sm={6}>
                                                <figure className="figure mb-0">
                                                    <img src={img4} className="figure-img img-fluid rounded" alt="..." />
                                                    <figcaption className="figure-caption">A caption for the above image.</figcaption>
                                                </figure>
                                            </Col>
                                            <Col sm={6}>
                                                <figure className="figure mb-0">
                                                    <img src={img5} className="figure-img img-fluid rounded" alt="..." />
                                                    <figcaption className="figure-caption text-end">A caption for the above image.</figcaption>
                                                </figure>
                                            </Col>
                                        </Row>

                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <FiguresExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={4}>
                            <Card>
                                <PreviewCardHeader title="Responsive Images" />

                                <CardBody>

                                    <p className="card-title-desc text-muted">Responsive Images with <code>img-fluid,max-width: 100%; and height: auto;</code> to the image so that it scales with the parent width.</p>

                                    <div className="live-preview">

                                        <div>
                                            <img src={img2} className="img-fluid" alt="Responsive" />
                                        </div>

                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <ResponsiveExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>
                </Container>
            </div >
        </React.Fragment >
    );
};

export default UiImages;
