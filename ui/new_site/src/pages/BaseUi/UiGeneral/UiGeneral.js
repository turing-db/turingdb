import React from 'react';
import { Button, Card, CardBody, Col, Container, Row, UncontrolledPopover, PopoverHeader, PopoverBody, UncontrolledTooltip, Spinner, Pagination, PaginationItem, PaginationLink, Breadcrumb, BreadcrumbItem } from 'reactstrap';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumbpage from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

import { PopoversExample, TooltipsExample, BreadcrumbExample, PaginationExample, SpinnersExample } from './UiGeneralCode';
import { Link } from 'react-router-dom';
const UiGeneral = () => {
    document.title="General | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">
               
                <Container fluid>
                    <BreadCrumbpage title="General" pageTitle="Base UI" />
                    <Row>
                        <Col lg={12}>
                            <Card>
                                <PreviewCardHeader title="Popovers" />
                                <CardBody>
                                    <p className="text-muted">Popovers example are available with follwing options , Directions are mirrored when using Bootstrap in RTL.</p>
                                    <div className="live-preview">
                                        <div className="hstack flex-wrap gap-2">
                                            <Button color="light" id="PopoverTop">
                                                Popover on top
                                            </Button>
                                            <UncontrolledPopover placement="top" target="PopoverTop" >
                                                <PopoverHeader> Top Popover </PopoverHeader>
                                                <PopoverBody> Vivamus sagittis lacus vel augue laoreet rutrum faucibus. </PopoverBody>
                                            </UncontrolledPopover>
                                            <Button color="light" id="PopoverRight">
                                                Popover on right
                                            </Button>
                                            <UncontrolledPopover placement="right" target="PopoverRight" >
                                                <PopoverHeader> Right Popover </PopoverHeader>
                                                <PopoverBody> Vivamus sagittis lacus vel augue laoreet rutrum faucibus. </PopoverBody>
                                            </UncontrolledPopover>

                                            <Button color="light" id="PopoverBottom">
                                                Popover on bottom
                                            </Button>
                                            <UncontrolledPopover placement="bottom" target="PopoverBottom" >
                                                <PopoverHeader> Bottom Popover </PopoverHeader>
                                                <PopoverBody> Vivamus sagittis lacus vel augue laoreet rutrum faucibus. </PopoverBody>
                                            </UncontrolledPopover>

                                            <Button color="light" id="PopoverLeft">
                                                Popover on left
                                            </Button>
                                            <UncontrolledPopover placement="left" target="PopoverLeft" >
                                                <PopoverHeader> Left Popover </PopoverHeader>
                                                <PopoverBody> Vivamus sagittis lacus vel augue laoreet rutrum faucibus. </PopoverBody>
                                            </UncontrolledPopover>

                                            <Button color="success" id="PopoverDismissible">
                                                Dismissible popover
                                            </Button>
                                            <UncontrolledPopover placement="right" target="PopoverDismissible" >
                                                <PopoverHeader> Dismissible popover </PopoverHeader>
                                                <PopoverBody> And here's some amazing content. It's very engaging. Right? </PopoverBody>
                                            </UncontrolledPopover>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <PopoversExample />
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
                                <PreviewCardHeader title="Tooltips" />

                                <CardBody>
                                    <p className="text-muted">Tooltip example are available with follwing options, Directions are mirrored when using Bootstrap in RTL.</p>
                                    <div className="live-preview">
                                        <div className="hstack flex-wrap gap-2">
                                            <Button color="light" id="tooltipTop">
                                                Tooltip on top
                                            </Button>
                                            <UncontrolledTooltip placement="top" target="tooltipTop" > Tooltip on top </UncontrolledTooltip>

                                            <Button color="light" id="tooltipRight">
                                                Tooltip on right
                                            </Button>
                                            <UncontrolledTooltip placement="right" target="tooltipRight" > Tooltip on right </UncontrolledTooltip>

                                            <Button color="light" id="tooltipBottom">
                                                Tooltip on bottom
                                            </Button>
                                            <UncontrolledTooltip placement="bottom" target="tooltipBottom" > Tooltip on bottom </UncontrolledTooltip>

                                            <Button color="light" id="tooltipLeft">
                                                Tooltip on left
                                            </Button>
                                            <UncontrolledTooltip placement="left" target="tooltipLeft" > Tooltip on left </UncontrolledTooltip>

                                            <Button color="primary" id="tooltipwithHtml">
                                                Tooltip with HTML
                                            </Button>
                                            <UncontrolledTooltip placement="top" target="tooltipwithHtml" > <em>Tooltip</em> <u>with</u> <b>HTML</b> </UncontrolledTooltip>

                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <TooltipsExample />
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
                                <PreviewCardHeader title="Breadcrumb" />

                                <CardBody>
                                    <p className="text-muted">Indicate the current page’s location within a navigational hierarchy</p>
                                    <div className="live-preview">
                                        <Breadcrumb listClassName='p-3 py-2 bg-light'>
                                            <BreadcrumbItem active> Home </BreadcrumbItem>
                                        </Breadcrumb>

                                        <Breadcrumb listClassName='p-3 py-2 bg-light'>
                                            <BreadcrumbItem> <Link to="#"> Home </Link> </BreadcrumbItem>
                                            <BreadcrumbItem active> Library </BreadcrumbItem>
                                        </Breadcrumb>

                                        <Breadcrumb listClassName='p-3 py-2 bg-light'>
                                            <BreadcrumbItem> <Link to="#"> Home </Link> </BreadcrumbItem>
                                            <BreadcrumbItem> <Link to="#"> Base UI </Link> </BreadcrumbItem>
                                            <BreadcrumbItem active> General </BreadcrumbItem>
                                        </Breadcrumb>

                                        <Breadcrumb listClassName='p-3 py-2 bg-light mb-0'>
                                            <BreadcrumbItem> <Link to="#"> <i className="ri-home-5-fill" /> </Link> </BreadcrumbItem>
                                            <BreadcrumbItem><Link to="#"> Base UI </Link> </BreadcrumbItem>
                                            <BreadcrumbItem active> General </BreadcrumbItem>
                                        </Breadcrumb>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BreadcrumbExample />
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
                                <PreviewCardHeader title="Pagination" />

                                <CardBody>

                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={6}>
                                                <h5 className="fs-15">Default Pagination</h5>
                                                <p className="text-muted">Use <code>Pagination</code> attribute to ul element to indicate a series of related content exists across multiple pages.</p>
                                                <Pagination>
                                                    <PaginationItem> <PaginationLink to="#"> Previous </PaginationLink> </PaginationItem>
                                                    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                    <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                    <PaginationItem> <PaginationLink to="#"> Next </PaginationLink> </PaginationItem>
                                                </Pagination>

                                                <Pagination>
                                                    <PaginationItem> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
                                                    <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                    <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                    <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                    <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
                                                </Pagination>
                                            </Col>


                                            <Col lg={6}>
                                                <div className="mt-4 mt-lg-0">
                                                    <h5 className="fs-15">Disabled and Active states</h5>
                                                    <p className="text-muted">Use <code>disabled</code> attribute to PaginationItem to links that appear
                                                        un-clickable and <code>active</code> class to
                                                        indicate the current page.</p>
                                                    <Pagination>
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
                                                    </Pagination>

                                                    <Pagination>
                                                        <PaginationItem disabled> <PaginationLink to="#"> <i className="mdi mdi-chevron-left" /> </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> <i className="mdi mdi-chevron-right" /> </PaginationLink> </PaginationItem>
                                                    </Pagination>
                                                </div>
                                            </Col>
                                        </Row>

                                        <Row>
                                            <Col lg={6}>
                                                <div className="mt-4">
                                                    <h5 className="fs-15">Sizing</h5>
                                                    <p className="text-muted">Use <code>pagination-lg</code> or <code>pagination-sm</code> to set different pagination sizes.</p>

                                                    {/* <!-- Pagination Large --> */}
                                                    <Pagination size='lg'>
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
                                                    </Pagination>

                                                    {/* <!-- Pagination Small --> */}
                                                    <Pagination size='sm'>
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
                                                    </Pagination>
                                                </div>

                                            </Col>

                                            <Col lg={6}>
                                                <div className="mt-4">
                                                    <h5 className="fs-15">Alignment</h5>

                                                    <p className="text-muted">Use <code>justify-content-start</code>, <code>justify-content-start</code>, or <code>justify-content-start</code>, class to pagination class to
                                                        change the alignment of pagination respectively.</p>

                                                    {/* <!-- Pagination Alignment --> */}


                                                    {/* <!-- Center Alignment --> */}
                                                    <Pagination listClassName="justify-content-center">
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
                                                    </Pagination>


                                                    {/* <!-- Right Alignment --> */}
                                                    <Pagination listClassName="justify-content-end">
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← &nbsp; Prev </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> Next &nbsp; → </PaginationLink> </PaginationItem>
                                                    </Pagination>

                                                </div>
                                            </Col>
                                        </Row>

                                        <Row>
                                            <Col lg={6}>
                                                <div className="mt-4">
                                                    <h5 className="fs-15">Custom Separated Pagination</h5>
                                                    <p className="text-muted">Use <code>pagination-separated</code> class to pagination class to set custom separated pagination.</p>

                                                    {/* <!-- Custom Separated Pagination Large --> */}
                                                    <Pagination className="pagination-lg pagination-separated">
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 4 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 5 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> → </PaginationLink> </PaginationItem>
                                                    </Pagination>

                                                    <Pagination className="pagination-separated">
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 4 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 5 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> → </PaginationLink> </PaginationItem>
                                                    </Pagination>

                                                    <Pagination className="pagination-sm pagination-separated">
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 4 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 5 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> → </PaginationLink> </PaginationItem>
                                                    </Pagination>
                                                </div>
                                            </Col>

                                            <Col lg={6}>
                                                <div className="mt-4">
                                                    <h5 className="fs-15">Custom Rounded Pagination</h5>
                                                    <p className="text-muted">Use <code>pagination-rounded</code> class to pagination class to set custom rounded pagination.</p>

                                                    {/* <!-- Pagination rounded --> */}

                                                    <Pagination className="pagination-lg pagination-rounded">
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 4 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 5 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> → </PaginationLink> </PaginationItem>
                                                    </Pagination>

                                                    <Pagination className="pagination-rounded">
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 4 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 5 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> → </PaginationLink> </PaginationItem>
                                                    </Pagination>

                                                    <Pagination className="pagination-sm pagination-rounded">
                                                        <PaginationItem disabled> <PaginationLink to="#"> ← </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 1 </PaginationLink> </PaginationItem>
                                                        <PaginationItem active> <PaginationLink to="#"> 2 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 3 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 4 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> 5 </PaginationLink> </PaginationItem>
                                                        <PaginationItem> <PaginationLink to="#"> → </PaginationLink> </PaginationItem>
                                                    </Pagination>
                                                </div>
                                            </Col>
                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <PaginationExample />
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
                                <PreviewCardHeader title="Spinners" />

                                <CardBody>

                                    <div className="live-preview">
                                        <Row>
                                            <Col lg={6}>
                                                <div>
                                                    <h5 className="fs-15">Border spinner</h5>
                                                    <p className="text-muted">Use <code>spinner-border</code> class for a lightweight loading indicator.</p>
                                                    <div className="d-flex flex-wrap gap-3 mb-2">
                                                        {/* <!-- Border spinner --> */}
                                                        <Spinner color="primary"> Loading... </Spinner>
                                                        <Spinner color="secondary"> Loading... </Spinner>
                                                        <Spinner color="success"> Loading... </Spinner>
                                                        <Spinner color="info"> Loading... </Spinner>
                                                        <Spinner color="warning"> Loading... </Spinner>
                                                        <Spinner color="danger"> Loading... </Spinner>
                                                        <Spinner color="dark"> Loading... </Spinner>
                                                        <Spinner color="light"> Loading... </Spinner>
                                                    </div>
                                                </div>
                                            </Col>


                                            <Col lg={6}>
                                                <div>
                                                    <h5 className="fs-15">Growing spinner</h5>
                                                    <p className="text-muted">Use <code>spinner-grow</code> class for a lightweight spinner with growing effect.</p>
                                                    <div className="d-flex flex-wrap gap-3 mb-2">

                                                        {/* <!-- Growing spinner --> */}
                                                        <Spinner color="primary" type="grow" > Loading... </Spinner>
                                                        <Spinner color="secondary" type="grow" > Loading... </Spinner>
                                                        <Spinner color="success" type="grow" > Loading... </Spinner>
                                                        <Spinner color="info" type="grow" > Loading... </Spinner>
                                                        <Spinner color="warning" type="grow" > Loading... </Spinner>
                                                        <Spinner color="danger" type="grow" > Loading... </Spinner>
                                                        <Spinner color="dark" type="grow" > Loading... </Spinner>
                                                        <Spinner color="light" type="grow" > Loading... </Spinner>
                                                    </div>
                                                </div>
                                            </Col>

                                        </Row>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <SpinnersExample />
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

export default UiGeneral;
