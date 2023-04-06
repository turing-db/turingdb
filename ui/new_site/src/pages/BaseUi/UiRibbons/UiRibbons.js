import React from 'react';
import { Card, CardBody, Col, Container, Row, } from 'reactstrap';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

import { RoundedRibbonExample, RibbonShapeExample, FilledRibbonsExample, BoxedRibbonsExample, RibbonsExample, RibbonsHoverExample } from './UiRibbonsCode';
const UiRibbons = () => {
    document.title="Ribbons | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Ribbons" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Rounded Ribbon" />
                                <CardBody>

                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <p className="text-muted">Use <code>round-shape</code> class to show round-shaped ribbon.</p>
                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-primary round-shape">Primary</div>
                                                        <h5 className="fs-14 text-end">Rounded Ribbon</h5>

                                                        <div className="ribbon-content mt-4 text-muted">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-success round-shape">Success</div>
                                                        <h5 className="fs-14 text-end">Rounded Ribbon</h5>
                                                        <div className="ribbon-content mt-4 text-muted">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>

                                                {/* <!-- Right Ribbon --> */}
                                                <div className="card ribbon-box border shadow-none right mb-lg-0">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-info round-shape">Info</div>
                                                        <h5 className="fs-14 text-start">Rounded Ribbon Right</h5>
                                                        <div className="ribbon-content mt-4 text-muted">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <RoundedRibbonExample />
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
                                <PreviewCardHeader title="Ribbon Shape" />

                                <CardBody>

                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <p className="text-muted">Use <code>ribbon-shape</code> class to show ribbon shaped ribbon.</p>
                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-primary ribbon-shape">Primary</div>
                                                        <h5 className="fs-14 text-end">Ribbon Shape</h5>
                                                        <div className="ribbon-content text-muted mt-4">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-success ribbon-shape">Success</div>
                                                        <h5 className="fs-14 text-end">Ribbon Shape</h5>
                                                        <div className="ribbon-content text-muted mt-4">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0 right">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-info ribbon-shape">Info</div>
                                                        <h5 className="fs-14 text-start">Ribbon Shape Right</h5>
                                                        <div className="ribbon-content text-muted mt-4">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <RibbonShapeExample />
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
                                <PreviewCardHeader title="Filled Ribbons" />

                                <CardBody>

                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <p className="text-muted">Use <code>ribbon-fill</code> class to show fill-shaped ribbon.</p>
                                            <Col xxl={4}>
                                                <div className="card ribbon-box border ribbon-fill shadow-none mb-lg-0">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-primary">New</div>
                                                        <h5 className="fs-14 text-end">Fill Ribbon</h5>
                                                        <div className="ribbon-content mt-3 text-muted">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box border ribbon-fill shadow-none mb-lg-0">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-success">Sale</div>
                                                        <h5 className="fs-14 text-end">Fill Ribbon</h5>
                                                        <div className="ribbon-content mt-3 text-muted">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>

                                                {/* <!-- Right Ribbon --> */}
                                                <div className="card ribbon-box border ribbon-fill shadow-none right mb-lg-0">
                                                    <CardBody>
                                                        <div className="ribbon ribbon-info">New</div>
                                                        <h5 className="fs-14 text-start">Fill Ribbon Right</h5>
                                                        <div className="ribbon-content mt-3 text-muted">
                                                            <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas
                                                                mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio.</p>
                                                        </div>
                                                    </CardBody>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <FilledRibbonsExample />
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
                                <PreviewCardHeader title="Boxed Ribbons" />

                                <CardBody>

                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <p className="text-muted">Use <code>ribbon-box</code> class with <code>ribbon-two</code> class at child element to show below-mentioned shaped ribbon.</p>
                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0">
                                                    <div className="card-body text-muted">
                                                        <div className="ribbon-two ribbon-two-primary"><span>Basic</span></div>
                                                        <h5 className="fs-14 text-end mb-3">Box Ribbon</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0">
                                                    <div className="card-body text-muted">
                                                        <div className="ribbon-two ribbon-two-success"><span>Standard</span></div>
                                                        <h5 className="fs-14 text-end mb-3">Box Ribbon</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>

                                                {/* <!-- Right Ribbon --> */}
                                                <div className="card ribbon-box border shadow-none mb-lg-0 right">
                                                    <div className="card-body text-muted">
                                                        <div className="ribbon-two ribbon-two-info"><span>Premium</span></div>
                                                        <h5 className="fs-14 mb-3">Box Ribbon Right</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus elementum ornare.</p>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BoxedRibbonsExample />
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
                                <PreviewCardHeader title="Ribbons" />

                                <CardBody>

                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <p className="text-muted">Use <code>ribbon-box</code> class with <code>ribbon-three</code> class at child element to show below-mentioned shaped ribbon.</p>
                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0">
                                                    <div className="card-body text-muted">
                                                        <span className="ribbon-three ribbon-three-primary"><span>Featured</span></span>
                                                        <h5 className="fs-14 text-end mb-3">Ribbon Shape</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0">
                                                    <div className="card-body text-muted">
                                                        <span className="ribbon-three ribbon-three-success"><span>Featured</span></span>
                                                        <h5 className="fs-14 text-end mb-3">Ribbon Shape</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none mb-lg-0 right">
                                                    <div className="card-body text-muted">
                                                        <span className="ribbon-three ribbon-three-info"><span>Featured</span></span>
                                                        <h5 className="fs-14 mb-3">Ribbon Right Shape</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <RibbonsExample />
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
                                <PreviewCardHeader title="Ribbons Hover" />

                                <CardBody>
                                    <div className="live-preview">
                                        <Row className="g-3">
                                            <p className="text-muted">Use <code>trending-ribbon</code> class to show ribbon with hovering effect.</p>
                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none overflow-hidden mb-lg-0">
                                                    <div className="card-body text-muted">
                                                        <div className="ribbon ribbon-info ribbon-shape trending-ribbon">
                                                            <span className="trending-ribbon-text">Trending</span> <i className="ri-flashlight-fill text-white align-bottom float-end ms-1"></i>
                                                        </div>
                                                        <h5 className="fs-14 text-end mb-3">Ribbon Shape</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box border shadow-none overflow-hidden mb-lg-0">
                                                    <div className="card-body text-muted">
                                                        <div className="ribbon ribbon-info ribbon-shape trending-ribbon">
                                                            <span className="trending-ribbon-text">Trending</span> <i className="ri-flashlight-fill text-white align-bottom float-end ms-1"></i>
                                                        </div>
                                                        <h5 className="fs-14 text-end mb-3">Ribbon Shape</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
                                                    </div>
                                                </div>
                                            </Col>

                                            <Col xxl={4}>
                                                <div className="card ribbon-box right border shadow-none overflow-hidden mb-lg-0">
                                                    <div className="card-body text-muted">
                                                        <div className="ribbon ribbon-info ribbon-shape trending-ribbon">
                                                            <i className="ri-flashlight-fill text-white align-bottom float-start me-1"></i> <span className="trending-ribbon-text">Trending</span>
                                                        </div>
                                                        <h5 className="fs-14 mb-3">Ribbon Right Shape</h5>
                                                        <p className="mb-0">Quisque nec turpis at urna dictum luctus. Suspendisse convallis dignissim eros at volutpat. In egestas mattis dui. Aliquam mattis dictum aliquet. Nulla sapien mauris, eleifend et sem ac, commodo dapibus odio. Vivamus pretium nec odio cursus.</p>
                                                    </div>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <RibbonsHoverExample />
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

export default UiRibbons;
