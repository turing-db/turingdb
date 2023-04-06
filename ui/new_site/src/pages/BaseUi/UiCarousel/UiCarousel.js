import React from 'react';
import { Card, CardBody, Col, Container, Row, } from 'reactstrap';

// Import Content
import UiContent from '../../../Components/Common/UiContent';
//import Components
import BreadCrumb from '../../../Components/Common/BreadCrumb';
import PreviewCardHeader from '../../../Components/Common/PreviewCardHeader';
import Slide from './CarouselTypes/slide';
import Slidewithcontrol from './CarouselTypes/slidewithcontrol';
import Slidewithindicator from './CarouselTypes/slidewithindicator';
import Slidewithcaption from './CarouselTypes/slidewithcaption';
import CrossfadeAnimation from './CarouselTypes/crossfadeAnimation';
import IndividualInterval from './CarouselTypes/individualInterval';
import DisableTouch from './CarouselTypes/disableTouch';
import SlideDark from './CarouselTypes/slideDark';
import { SlideOnlyExample, WithControlExample, WithIndicatorExample, WithCaptionExample, CrossFadeExample, InduvidualIntervalExample, DisableTouchExample, DarkVariantExample } from './UiCarouselCode';

const UiCarousel = () => {
    document.title="Carousel | Velzon - React Admin & Dashboard Template";
    return (
        <React.Fragment>
            <UiContent />
            <div className="page-content">                
                <Container fluid>
                    <BreadCrumb title="Carousel" pageTitle="Base UI" />
                    <Row>
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Slides Only" />
                                <CardBody>                                    
                                    <div className="live-preview">
                                        <Slide />
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <SlideOnlyExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="With Controls" />
                                <CardBody>                                    
                                    <div className="live-preview">
                                        <Slidewithcontrol />
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "375px" }}>
                                            <code>
                                                <WithControlExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="with Indicators" />

                                <CardBody>                                    
                                    <div className="live-preview">
                                        <Slidewithindicator />
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup" style={{ height: "375px" }}>
                                            <code>
                                                <WithIndicatorExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="with Captions" />
                                <CardBody>                                    
                                    <div className="live-preview" >
                                        <Slidewithcaption />
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <WithCaptionExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Crossfade Animation" />
                                <CardBody>                                    
                                    <div className="live-preview">
                                        <CrossfadeAnimation />
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <CrossFadeExample />
                                            </code>
                                        </pre>
                                    </div>

                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Individual carousel-item Interval" />

                                <CardBody>                                    
                                    <div className="live-preview">
                                        <IndividualInterval />
                                    </div>

                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <InduvidualIntervalExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>
                    </Row>

                    <Row>
                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Disable Touch Swiping" />
                                <CardBody>                                    
                                    <div className="live-preview">
                                        <DisableTouch />
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <DisableTouchExample />
                                            </code>
                                        </pre>
                                    </div>
                                </CardBody>
                            </Card>
                        </Col>

                        <Col xl={6}>
                            <Card>
                                <PreviewCardHeader title="Dark Variant" />
                                <CardBody>                                    
                                    <div className="live-preview">
                                        <SlideDark />
                                    </div>
                                    <div className="d-none code-view">
                                        <pre className="language-markup">
                                            <code>
                                                <DarkVariantExample />
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

export default UiCarousel;
