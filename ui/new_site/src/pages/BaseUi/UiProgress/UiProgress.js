import React from 'react';
import { Button, Card, CardBody, Col, Container, Progress, Row, } from 'reactstrap';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';

import { DefaultProgressExample, BackgroundColorExample, LabelExample, MultipleBarsExample, HeightExample, StripedExample, AnimatedStripedExample, GradientExample, AnimatedExample, CustomExample, CustomProgressExample, ContentExample, ProgressWithStepExample, StepProgressArrowExample } from './UiProgressCode';

const UiProgress = () => {
    document.title="Progress | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Progress" pageTitle="Base UI" />
                    <Row>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Default Progress" />

                                <CardBody>
                                    <p className="text-muted">Use <code>progress</code> class to show default progress.</p>

                                    <div className="live-preview">
                                        <div className="mb-4">
                                            <Progress value={0} />
                                        </div>
                                        <div className="mb-4">
                                            <Progress value={25} />

                                        </div>
                                        <div className="mb-4">
                                            <Progress value={50} />
                                        </div>
                                        <div className="mb-4">
                                            <Progress value={75} />
                                        </div>
                                        <div>
                                            <Progress value={100} />
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <DefaultProgressExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Progress with background color" />

                                <CardBody>

                                    <p className="text-muted">Use <code>bg-</code> class to progress bar class with the below-mentioned color variation to set the background color progress bar.</p>

                                    <div className="live-preview">
                                        <div className="mb-4">
                                            <Progress color="primary" value={15} />
                                        </div>
                                        <div className="mb-4">
                                            <Progress color="success" value={25} />
                                        </div>
                                        <div className="mb-4">
                                            <Progress color="info" value={50} />
                                        </div>
                                        <div className="mb-4">
                                            <Progress color="warning" value={75} />
                                        </div>
                                        <div>
                                            <Progress color="danger" value={100} />
                                        </div>
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <BackgroundColorExample />
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
                                <PreviewCardHeader title="Progress with Label" />

                                <CardBody>

                                    <p className="text-muted">Add labels to your progress bars by placing text within the progress-bar.</p>

                                    <div className="live-preview">
                                        <Progress color="primary" value={25}> 25% </Progress>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ maxHeight: "275px" }}>
                                            <code>
                                                <LabelExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Multiple Bars" />

                                <CardBody>
                                    <p className="text-muted">Use <code>bg-</code> class to progress bar class with the below-mentioned color to change the appearance of individual progress bars.</p>

                                    <div className="live-preview">
                                        <div>
                                            <Progress multi>
                                                <Progress bar value="15" />
                                                <Progress bar color="success" value="30" />
                                                <Progress bar color="info" value="20" />
                                            </Progress>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ maxHeight: "275px" }}>
                                            <code>
                                                <MultipleBarsExample />
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
                                <PreviewCardHeader title="Height" />

                                <CardBody>

                                    <p className="text-muted">Use <code>progress-sm</code>, <code>progress-lg</code>, or <code>progress-xl</code> class to set the different heights of progress.</p>
                                    <div className="live-preview">
                                        <div className="mb-4">
                                            <h5 className="fs-13">Small Progress</h5>
                                            <Progress color="primary" value={25} className="progress-sm" />
                                        </div>
                                        <div className="mb-4">
                                            <h5 className="fs-13">Default Progress </h5>
                                            <Progress color="success" value={40} className="progress-md" />
                                        </div>
                                        <div className="mb-4">
                                            <h5 className="fs-13">Large Progress</h5>
                                            <Progress color="warning" value={50} className="progress-lg" />
                                        </div>
                                        <div>
                                            <h5 className="fs-13">Extra Large Progress</h5>
                                            <Progress color="danger" value={70} className="progress-xl" />
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <HeightExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Striped Progress" />

                                <CardBody>

                                    <p className="text-muted">Use <code>progress-bar-striped</code>class to add strip to the progress.</p>
                                    <div className="live-preview">
                                        <div className="mb-4">
                                            <Progress striped value={25} />
                                        </div>
                                        <div>
                                            <Progress color="success" striped value={40} />
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ maxHeight: "275px" }}>
                                            <code>
                                                <StripedExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>

                            <Card>
                                <PreviewCardHeader title="Animated Striped Progress" />

                                <CardBody>

                                    <p className="text-muted">Use <code>progress-bar-striped progress-bar-animated</code> class to add strip with animation to the progress.</p>

                                    <div className="live-preview">
                                        <div>
                                            <Progress value={75} striped animated />
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ maxHeight: "275px" }}>
                                            <code>
                                                <AnimatedStripedExample />
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
                                <PreviewCardHeader title="Gradient Progress" />

                                <CardBody>

                                    <p className="text-muted">Use <code>bg-gradient</code> class to show gradient progress bar.</p>

                                    <div className="live-preview">
                                        <div className="mb-4">
                                            <Progress value={15} className="bg-gradient" />
                                        </div>
                                        <div className="mb-4">
                                            <Progress color="success" value={25} className="bg-gradient" />
                                        </div>
                                        <div className="mb-4">
                                            <Progress color="info" value={50} className="bg-gradient" />
                                        </div>
                                        <div className="mb-4">
                                            <Progress color="warning" value={75} className="bg-gradient" />
                                        </div>
                                        <div>
                                            <Progress color="danger" value={100} className="bg-gradient" />
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <GradientExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Animated Progress" />

                                <CardBody>
                                    <p className="text-muted">Use <code>animated-progress</code> class to show progress with animation.</p>
                                    <div className="live-preview">
                                        <div className="mb-4">
                                            <Progress value={15} className="animated-progess" />

                                        </div>
                                        <div className="mb-4">
                                            <Progress value={25} color="success" className="animated-progess" />
                                        </div>
                                        <div className="mb-4">
                                            <Progress value={50} color="info" className="animated-progess" />
                                        </div>
                                        <div className="mb-4">
                                            <Progress value={75} color="warning" className="animated-progess" />
                                        </div>
                                        <div>
                                            <Progress value={100} color="danger" className="animated-progess" />
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <AnimatedExample />
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
                                <PreviewCardHeader title="Custom Progress" />

                                <CardBody>
                                    <p className="text-muted">Use <code>animated-progess custom-progess </code> class to show custom progress with animation.</p>
                                    <div className="live-preview">
                                        <div className="mb-4">
                                            <Progress value={15} color="primary" className="animated-progess custom-progress" />
                                        </div>
                                        <div className="mb-4">
                                            <Progress value={25} color="success" className="animated-progess custom-progress" />
                                        </div>
                                        <div className="mb-4">
                                            <Progress value={50} color="info" className="animated-progess custom-progress" />
                                        </div>
                                        <div className="mb-4">
                                            <Progress value={75} color="warning" className="animated-progess custom-progress" />
                                        </div>
                                        <div>
                                            <Progress value={100} color="danger" className="animated-progess custom-progress" />
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <CustomExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Custom Progress with Label" />

                                <CardBody>
                                    <p className="text-muted">Use <code>animated-progress custom-progess progress-label</code> class to show custom progress with animation and label.</p>
                                    <div className="live-preview">
                                        <div className="d-flex align-items-center pb-2 mt-4">
                                            <div className="flex-shrink-0 me-3">
                                                <div className="avatar-xs">
                                                    <div className="avatar-title bg-light rounded-circle text-muted fs-16">
                                                        <i className="mdi mdi-facebook"></i>
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex-grow-1">
                                                <div>
                                                    <Progress value={15} color="primary" className="animated-progess custom-progress progress-label" ><div className="label">15%</div> </Progress>
                                                </div>
                                            </div>
                                        </div>

                                        <div className="d-flex align-items-center py-2">
                                            <div className="flex-shrink-0 me-3">
                                                <div className="avatar-xs">
                                                    <div className="avatar-title bg-light rounded-circle text-muted fs-16">
                                                        <i className="mdi mdi-twitter"></i>
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex-grow-1">
                                                <div>
                                                    <Progress value={25} color="success" className="animated-progess custom-progress progress-label" ><div className="label">25%</div> </Progress>
                                                </div>
                                            </div>
                                        </div>

                                        <div className="d-flex align-items-center py-2">
                                            <div className="flex-shrink-0 me-3">
                                                <div className="avatar-xs">
                                                    <div className="avatar-title bg-light rounded-circle text-muted fs-16">
                                                        <i className="mdi mdi-github"></i>
                                                    </div>
                                                </div>
                                            </div>
                                            <div className="flex-grow-1">
                                                <div>
                                                    <Progress value={50} color="info" className="animated-progess custom-progress progress-label" ><div className="label">30%</div> </Progress>
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <CustomProgressExample />
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
                                <PreviewCardHeader title="Content Progress" />
                                <CardBody>
                                    <p className="text-muted pb-2">Example of progress with content wrapped in progress.</p>

                                    <div className="live-preview">

                                        <Card className="bg-light overflow-hidden shadow-none">
                                            <CardBody>
                                                <div className="d-flex">
                                                    <div className="flex-grow-1">
                                                        <h6 className="mb-0"><b className="text-secondary">30%</b> Update in
                                                            progress...</h6>
                                                    </div>
                                                    <div className="flex-shrink-0">
                                                        <h6 className="mb-0">1 min left</h6>
                                                    </div>
                                                </div>
                                            </CardBody>
                                            <div >
                                                <Progress value={30} color="secondary" className="bg-soft-secondary rounded-0" />
                                            </div>
                                        </Card>

                                        <Card className="bg-light overflow-hidden shadow-none">
                                            <CardBody>
                                                <div className="d-flex">
                                                    <div className="flex-grow-1">
                                                        <h6 className="mb-0"><b className="text-success">60%</b> Update in
                                                            progress...</h6>
                                                    </div>
                                                    <div className="flex-shrink-0">
                                                        <h6 className="mb-0">45s left</h6>
                                                    </div>
                                                </div>
                                            </CardBody>
                                            <div>
                                                <Progress value={60} color="success" className="bg-soft-success rounded-0" />
                                            </div>
                                        </Card>

                                        <Card className="bg-light overflow-hidden shadow-none">
                                            <CardBody>
                                                <div className="d-flex">
                                                    <div className="flex-grow-1">
                                                        <h6 className="mb-0"><b className="text-danger">82%</b> Update in
                                                            progress...</h6>
                                                    </div>
                                                    <div className="flex-shrink-0">
                                                        <h6 className="mb-0">25s left</h6>
                                                    </div>
                                                </div>
                                            </CardBody>
                                            <div>
                                                <Progress value={82} color="danger" className="bg-soft-danger rounded-0" />
                                            </div>
                                        </Card>

                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "275px" }}>
                                            <code>
                                                <ContentExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xxl={6}>
                            <Card>
                                <PreviewCardHeader title="Progress with Steps" />

                                <CardBody>
                                    <p className="text-muted pb-2">Here is the example of progress which is represented by steps completion.</p>

                                    <div className="live-preview">
                                        <div className="position-relative m-4">
                                            <Progress value={50} style={{ height: "1px" }} />
                                            <Button size="sm" color="primary" className="position-absolute top-0 start-0 translate-middle rounded-pill" style={{ width: "2rem", height: "2rem" }}>1</Button>
                                            <Button size="sm" color="primary" className="position-absolute top-0 start-50 translate-middle rounded-pill" style={{ width: "2rem", height: "2rem" }}>2</Button>
                                            <Button size="sm" color="light" className="position-absolute top-0 start-100 translate-middle rounded-pill" style={{ width: "2rem", height: "2rem" }}>3</Button>
                                        </div>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ maxHeight: "275px" }}>
                                            <code>
                                                <ProgressWithStepExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>

                            <Card>
                                <PreviewCardHeader title="Step Progress with Arrow" />

                                <CardBody>
                                    <p className="text-muted pb-2">Use <code>progress-step-arrow </code>class to create step progress with an arrow.</p>

                                    <div className="live-preview">
                                        <Progress multi className='progress-step-arrow progress-info'>
                                            <Progress bar value="35"> Step 1 </Progress>
                                            <Progress bar value="35"> Step 2 </Progress>
                                            <Progress bar value="35" color="light" className="text-dark"> Step 3 </Progress>
                                        </Progress>
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ maxHeight: "275px" }}>
                                            <code>
                                                <StepProgressArrowExample />
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

export default UiProgress;
